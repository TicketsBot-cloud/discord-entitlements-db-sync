package daemon

import (
	"context"
	"time"

	"github.com/TicketsBot-cloud/common/collections"
	"github.com/TicketsBot-cloud/common/model"
	"github.com/TicketsBot-cloud/common/utils"
	"github.com/TicketsBot-cloud/database"
	"github.com/TicketsBot-cloud/discord-entitlements-db-sync/internal/config"
	"github.com/TicketsBot-cloud/gdl/objects/entitlement"
	"github.com/TicketsBot-cloud/gdl/rest"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

type Daemon struct {
	config config.Config
	db     *database.Database
	logger *zap.Logger
}

func NewDaemon(config config.Config, db *database.Database, logger *zap.Logger) *Daemon {
	return &Daemon{
		config: config,
		db:     db,
		logger: logger,
	}
}

func (d *Daemon) Start() error {
	d.logger.Info("Starting daemon", zap.Duration("frequency", d.config.RunFrequency))
	ctx := context.Background()

	timer := time.NewTimer(d.config.RunFrequency)
	defer timer.Stop()

	for {
		select {
		case <-timer.C:
			start := time.Now()
			if err := d.doRun(ctx, d.config.ExecutionTimeout); err != nil {
				d.logger.Error("Failed to run", zap.Error(err))
			}

			d.logger.Info("Run completed", zap.Duration("duration", time.Since(start)))

			timer.Reset(d.config.RunFrequency)
		case <-ctx.Done():
			d.logger.Info("Shutting down daemon")
			return nil
		}
	}
}

func (d *Daemon) doRun(ctx context.Context, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	return d.RunOnce(ctx)
}

func (d *Daemon) RunOnce(ctx context.Context) error {
	d.logger.Debug("Running synchronisation")

	start := time.Now()
	defer func() {
		duration := time.Now().Sub(start)
		if duration > (d.config.ExecutionTimeout / 2.0) {
			d.logger.Warn("Execution took more than 50% of the timeout", zap.Duration("duration", duration))
		}
	}()

	activeEntitlements, err := d.fetchEntitlements(ctx)
	if err != nil {
		d.logger.Error("Failed to fetch entitlements", zap.Error(err))
		return err
	}

	d.logger.Debug("Fetched entitlements", zap.Int("count", len(activeEntitlements)))

	skuCache := make(map[uint64]model.Sku)
	unknownSkus := collections.NewSet[uint64]()

	tx, err := d.db.BeginTx(ctx)
	if err != nil {
		return err
	}

	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*15)
		defer cancel()

		tx.Rollback(ctx)
	}()

	for _, entitlement := range activeEntitlements {
		if unknownSkus.Contains(entitlement.SkuId) {
			d.logger.Debug("Skipping unknown SKU", zap.Uint64("sku_id", entitlement.SkuId))
			continue
		}

		sku, ok := skuCache[entitlement.SkuId]
		if !ok {
			tmp, err := d.db.DiscordStoreSkus.GetSku(ctx, entitlement.SkuId)
			if err != nil {
				d.logger.Error("Failed to get SKU ID", zap.Uint64("sku_id", entitlement.SkuId), zap.Error(err))
				return err
			}

			if tmp == nil {
				unknownSkus.Add(entitlement.SkuId)
				d.logger.Debug("Sku not found in discord_store_skus", zap.Uint64("discord_id", entitlement.SkuId))
				continue
			}

			sku = *tmp
			skuCache[entitlement.SkuId] = sku
		}

		if entitlement.Deleted {
			entitlementId, err := d.db.DiscordEntitlements.GetEntitlementId(ctx, tx, entitlement.Id)
			if err != nil {
				d.logger.Error("Failed to get entitlement ID", zap.Uint64("discord_id", entitlement.Id), zap.Error(err))
				return err
			}

			if entitlementId != nil {
				d.logger.Info("Found deleted entitlement", zap.Uint64("discord_id", entitlement.Id), zap.String("entitlement_id", entitlementId.String()))

				if err := d.db.Entitlements.DeleteById(ctx, tx, *entitlementId); err != nil {
					d.logger.Error("Failed to delete entitlement", zap.Error(err))
					return err
				}
			}

			continue
		}

		created, err := d.db.Entitlements.Create(ctx, tx, entitlement.GuildId, entitlement.UserId, sku.Id, model.EntitlementSourceDiscord, entitlement.EndsAt)
		if err != nil {
			d.logger.Error("Failed to create entitlement", zap.Error(err))
			return err
		}

		// Link entitlement to discord ID
		if err := d.db.DiscordEntitlements.Create(ctx, tx, entitlement.Id, created.Id); err != nil {
			d.logger.Error("Failed to link entitlement", zap.Error(err))
			return err
		}

		d.logger.Debug("Created entitlement", zap.Uint64("discord_id", entitlement.Id), zap.Any("entitlement", created))
	}

	// Delete missing entitlements (e.g. test entitlements)
	allEntitlements, err := d.db.DiscordEntitlements.ListAll(ctx, tx)
	if err != nil {
		d.logger.Error("Failed to list all discord entitlements", zap.Error(err))
		return err
	}

	activeEntitlementsSet := collections.NewSet[uint64]()
	for _, entitlement := range activeEntitlements {
		activeEntitlementsSet.Add(entitlement.Id)
	}

	toDelete := make([]uuid.UUID, 0)
	for discordId, entitlementId := range allEntitlements {
		if !activeEntitlementsSet.Contains(discordId) {
			toDelete = append(toDelete, entitlementId)
		}
	}

	if len(toDelete) >= d.config.MaxRemovalsThreshold {
		d.logger.Error("MAX_REMOVALS_THRESHOLD exceeded, not deleting entitlements", zap.Int("count", len(toDelete)), zap.Int("threshold", d.config.MaxRemovalsThreshold))
	} else {
		for _, entitlementId := range toDelete {
			d.logger.Info("Deleting missing entitlement", zap.String("entitlement_id", entitlementId.String()))

			if err := d.db.Entitlements.DeleteById(ctx, tx, entitlementId); err != nil {
				d.logger.Error("Failed to delete entitlement", zap.Error(err))
				return err
			}
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return err
	}

	return nil
}

func (d *Daemon) fetchEntitlements(ctx context.Context) ([]entitlement.Entitlement, error) {
	return d.nextPage(ctx, 0, nil)
}

const pageLimit = 100

func (d *Daemon) nextPage(ctx context.Context, afterId uint64, entitlements []entitlement.Entitlement) ([]entitlement.Entitlement, error) {
	d.logger.Debug("Fetching page of entitlements", zap.Uint64("after", afterId), zap.Int("limit", pageLimit), zap.Int("total", len(entitlements)))

	fetched, err := rest.ListEntitlements(ctx, d.config.Discord.Token, nil, d.config.Discord.ApplicationId, rest.EntitlementQueryOptions{
		After:         utils.Ptr(afterId),
		Limit:         utils.Ptr(pageLimit),
		ExcludedEnded: utils.Ptr(true),
	})
	if err != nil {
		return nil, err
	}

	entitlements = append(entitlements, fetched...)

	if len(fetched) < pageLimit {
		return entitlements, nil
	} else {
		return d.nextPage(ctx, fetched[len(fetched)-1].Id, entitlements)
	}
}
