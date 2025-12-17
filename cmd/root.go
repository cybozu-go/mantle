package cmd

import (
	"os"

	"github.com/cybozu-go/mantle/cmd/backup"
	"github.com/cybozu-go/mantle/cmd/controller"
	"github.com/cybozu-go/mantle/cmd/webhook"
	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use: "mantle",
}

func init() {
	rootCmd.AddCommand(backup.BackupCmd)
	rootCmd.AddCommand(controller.ControllerCmd)
	rootCmd.AddCommand(webhook.WebhookCmd)
}

func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}
