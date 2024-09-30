package innodb

import (
	"github.com/spf13/cobra"
)

const innodbToolsShortDescription = "(DANGEROUS) innodb tools"
const innodbToolsLongDescription = "xbstream tools allows to interact with local xbstream backups. " +
	"Be aware that this command can do potentially harmful operations and make sure that you know what you're doing."

var (
	InnodbToolsCmd = &cobra.Command{
		Use:    "innodb",
		Short:  innodbToolsShortDescription,
		Long:   innodbToolsLongDescription,
		Hidden: true,
	}
)
