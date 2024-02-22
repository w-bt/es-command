package main

import (
	"crm-es/command"
	"crm-es/pkg/logger"
	"github.com/spf13/cobra"
)

func newCLI() *cobra.Command {
	cli := &cobra.Command{
		Use:   "crm-es",
		Short: "es command for migration",
	}

	cli.AddCommand(getAliasesCmd())
	cli.AddCommand(getThresholdCmd())
	cli.AddCommand(duplicateAllCmd())
	cli.AddCommand(duplicateCmd())
	cli.AddCommand(removeAllDuplicateIndicesCmd())
	cli.AddCommand(removeCmd())
	cli.AddCommand(reindexAllCmd())
	cli.AddCommand(reindexCmd())
	cli.AddCommand(getTaskAllCmd())
	cli.AddCommand(getTaskCmd())
	cli.AddCommand(initialSnapshotCmd())
	cli.AddCommand(specificSnapshotCmd())
	cli.AddCommand(mergeAllCmd())
	cli.AddCommand(mergeCmd())
	cli.AddCommand(checkProgressRecoveryCmd())
	cli.AddCommand(copySettingCmd())
	cli.AddCommand(addAliasCmd())
	cli.AddCommand(manualReindexAllCmd())
	cli.AddCommand(manualReindexAllV2Cmd())
	cli.AddCommand(manualReindexChunkCmd())
	cli.AddCommand(manualReindexOrganizationCmd())
	cli.AddCommand(shardDistributionCmd())
	cli.AddCommand(checkRoutingShardCmd())
	cli.AddCommand(nodeLoadCmd())

	return cli
}

func getAliasesCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "aliases",
		Short: "Get all aliases",
		Run: func(_ *cobra.Command, _ []string) {
			logger.Infof("Start get aliases ...")
			command.GetAliases()
			logger.Infof("Finish get aliases ...")
		},
	}
}

func getThresholdCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "threshold",
		Short: "Get all thresholds",
		Run: func(_ *cobra.Command, _ []string) {
			logger.Infof("Start get thresholds ...")
			command.CheckThreshold()
			logger.Infof("Finish get thresholds ...")
		},
	}
}

func duplicateAllCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "duplicate_all",
		Short: "Duplicate index",
		Run: func(_ *cobra.Command, _ []string) {
			logger.Infof("Start duplicating ...")
			command.DuplicateAll()
			logger.Infof("Finish duplicating ...")
		},
	}
}

func duplicateCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "duplicate",
		Short: "Duplicate index",
		Run: func(_ *cobra.Command, args []string) {
			if len(args) != 1 {
				logger.Fatalf("missing index name")
			}
			indexName := args[0]
			logger.Infof("Start duplicating %s ...", indexName)
			command.Duplicate(indexName)
			logger.Infof("Finish duplicating %s ...", indexName)
		},
	}
}

func removeAllDuplicateIndicesCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "remove_all",
		Short: "Remove all duplicate index",
		Run: func(_ *cobra.Command, _ []string) {
			logger.Infof("Start removing ...")
			command.RemoveAll()
			logger.Infof("Finish removing ...")
		},
	}
}

func removeCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "remove",
		Short: "Remove duplicate index",
		Run: func(_ *cobra.Command, args []string) {
			if len(args) != 1 {
				logger.Fatalf("missing index name")
			}
			indexName := args[0]
			logger.Infof("Start removing %s ...", indexName)
			command.Remove(indexName)
			logger.Infof("Finish removing %s ...", indexName)
		},
	}
}

func reindexAllCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "reindex_all",
		Short: "Reindex all indices (format: 2023-03-19T08:00:00.000)",
		Run: func(_ *cobra.Command, args []string) {
			if len(args) != 1 {
				logger.Fatalf("missing updated at threshold")
			}
			updatedAt := args[0]
			logger.Infof("Start reindexing ...")
			command.ReindexAll(updatedAt)
			logger.Infof("Finish reindexing ...")
		},
	}
}

func reindexCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "reindex",
		Short: "Reindex index (format: source_index source_alias dest_index 2023-03-19T08:00:00.000)",
		Run: func(_ *cobra.Command, args []string) {
			if len(args) != 4 {
				logger.Fatalf("missing source_index or dest_index or updated_at")
			}
			sourceIndex := args[0]
			sourceAlias := args[1]
			destIndex := args[2]
			updatedAt := args[3]
			logger.Infof("Start reindexing %s to %s ...", sourceIndex, destIndex)
			command.Reindex(sourceIndex, sourceAlias, destIndex, updatedAt)
			logger.Infof("Finish reindexing %s to %s ...", sourceIndex, destIndex)
		},
	}
}

func getTaskAllCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "task_all",
		Short: "Get all task statuses",
		Run: func(_ *cobra.Command, _ []string) {
			logger.Infof("Start getting task statuses ...")
			command.GetTaskAll()
			logger.Infof("Finish getting task statuses ...")
		},
	}
}

func getTaskCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "task",
		Short: "Get task status",
		Run: func(_ *cobra.Command, args []string) {
			if len(args) != 1 {
				logger.Fatalf("missing task id")
			}
			taskID := args[0]
			logger.Infof("Start getting task status for id %s ...", taskID)
			command.GetTask(taskID)
			logger.Infof("Finish getting task status for id %s ...", taskID)
		},
	}
}

func initialSnapshotCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "snapshot_all",
		Short: "Snapshot all",
		Run: func(_ *cobra.Command, _ []string) {
			logger.Infof("Start snapshot all ...")
			command.InitialSnapshotAll()
			logger.Infof("Finish snapshot all ...")
		},
	}
}

func specificSnapshotCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "specific_snapshot",
		Short: "Specific snapshot for duplicated indices",
		Run: func(_ *cobra.Command, _ []string) {
			logger.Infof("Start snapshot duplicated indices ...")
			command.SpecificSnapshot()
			logger.Infof("Finish snapshot duplicated indices ...")
		},
	}
}

func mergeAllCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "merge_all",
		Short: "Merge all indices",
		Run: func(_ *cobra.Command, _ []string) {
			logger.Infof("Start merging all ...")
			command.MergeAll()
			logger.Infof("Finish merging all ...")
		},
	}
}

func mergeCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "merge",
		Short: "Merge index",
		Run: func(_ *cobra.Command, args []string) {
			if len(args) != 2 {
				logger.Fatalf("missing source_index or dest_index")
			}
			sourceIndex := args[0]
			destIndex := args[1]
			logger.Infof("Start merging ...")
			command.Merge(sourceIndex, destIndex)
			logger.Infof("Finish merging ...")
		},
	}
}

func checkProgressRecoveryCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "check_recovery",
		Short: "check recovery progress",
		Run: func(_ *cobra.Command, args []string) {
			if len(args) != 1 {
				logger.Fatalf("missing snapshot name")
			}
			name := args[0]
			logger.Infof("Start merging ...")
			command.Recovery(name)
			logger.Infof("Finish merging ...")
		},
	}
}

func copySettingCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "copy",
		Short: "copy",
		Run: func(_ *cobra.Command, _ []string) {
			logger.Infof("Start copy ...")
			command.Copy()
			logger.Infof("Finish copy ...")
		},
	}
}

func addAliasCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "add_alias",
		Short: "add alias",
		Run: func(_ *cobra.Command, _ []string) {
			logger.Infof("Start adding alias ...")
			command.AddAlias()
			logger.Infof("Finish adding alias ...")
		},
	}
}

func manualReindexAllCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "manual_reindex_all",
		Short: "manual reindex all",
		Run: func(_ *cobra.Command, _ []string) {
			logger.Infof("Start reindex all ...")
			command.ManualReindexAll()
			logger.Infof("Finish reindex all ...")
		},
	}
}

func manualReindexAllV2Cmd() *cobra.Command {
	return &cobra.Command{
		Use:   "manual_reindex_all_v2",
		Short: "manual reindex all",
		Run: func(_ *cobra.Command, args []string) {
			refresh := "false"
			if len(args) != 1 {
				refresh = args[0]
			}
			logger.Infof("Start reindex all ...")
			command.ManualReindexAllV2(refresh)
			logger.Infof("Finish reindex all ...")
		},
	}
}

func manualReindexChunkCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "manual_reindex_chunk",
		Short: "manual reindex chunk (format: 2023-09-05T00:00:00.000 true/false/wait_for)",
		Run: func(_ *cobra.Command, args []string) {
			if len(args) != 2 {
				logger.Fatalf("missing date threshold and force refresh")
			}
			date := args[0]
			refresh := args[1]
			logger.Infof("Start reindex chunk ...")
			command.ManualReindexChunk(date, refresh)
			logger.Infof("Finish reindex chunk ...")
		},
	}
}

func manualReindexOrganizationCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "manual_reindex_organization",
		Short: "manual reindex organization (format: 2023-09-05T00:00:00.000 organization_id)",
		Run: func(_ *cobra.Command, args []string) {
			if len(args) != 2 {
				logger.Fatalf("missing date threshold and force refresh")
			}
			date := args[0]
			organizationID := args[1]
			logger.Infof("Start reindex organization %s...", organizationID)
			command.ManualReindexOrganization(date, organizationID)
			logger.Infof("Finish reindex organization %s...", organizationID)
		},
	}
}

func shardDistributionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "shard_distribution",
		Short: "shard_distribution",
		Run: func(_ *cobra.Command, _ []string) {
			logger.Infof("Start checking shards...")
			command.ShardDistribution()
			logger.Infof("Finish checking shards...")
		},
	}
}

func checkRoutingShardCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "shard_routing",
		Short: "shard_routing",
		Run: func(_ *cobra.Command, _ []string) {
			logger.Infof("Start checking index routing...")
			command.CheckRoutingShards()
			logger.Infof("Finish checking index routing...")
		},
	}
}

func nodeLoadCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "node_load",
		Short: "node_load",
		Run: func(_ *cobra.Command, _ []string) {
			logger.Infof("Start checking node load...")
			command.NodeLoad()
			logger.Infof("Finish checking node load...")
		},
	}
}
