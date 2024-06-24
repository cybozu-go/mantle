package cmd

import (
	"os"

	"github.com/cybozu-go/mantle/cmd/backupandrotate"
	"github.com/cybozu-go/mantle/cmd/controller"
	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use: "mantle",
}

func init() {
	rootCmd.AddCommand(backupandrotate.BackupAndRotateCmd)
	rootCmd.AddCommand(controller.ControllerCmd)
}

func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}
