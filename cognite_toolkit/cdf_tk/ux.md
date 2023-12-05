

# Structure today

```bash

cdf-tk 
    
    # globals
    --help #global help
    --version #global version
    --verbose | --no-verbose # Turn on to get more verbose output [default: no-verbose] 
   --override-env | --no-override-env # Use .env file to override current environment variables [default: no-override-env] 
   --cluster # Cognite Data Fusion cluster to use [env var: CDF_CLUSTER] [default: None] 
   --project # Cognite Data Fusion project to use [env var: CDF_PROJECT] [default: None]
   --install-completion # Install completion for the current shell. 
   --show-completion # Show completion for the current shell, to copy it or customize the installation.
    
    # subcommands
    init init_dir
        --help # subcommand help
        --dry-run # Whether to do a dry-run, do dry-run if present                                                                                 
        --upgrade # Will upgrade templates in place without overwriting config.yaml files                                                          
        --git # Will download the latest templates from the git repository branch specified. Use `main` to get the very latest templates.      
        --no-backup # Will skip making a backup before upgrading                                                                                     
        --clean # Will delete the new_project directory before starting                                                                          
        
    auth
        --help # subcommand help
        verify
            --help # subcommand help
            --dry-run # Whether to do a dry-run, do dry-run if present
            --interactive # Will run the verification in interactive mode, prompting for input
            --group-file 
            --update-group
            --create-group

    build
        --help # subcommand help
        --env # env defined in local.yaml
        --build-dir # build dir default .build
        --clean # delete previous builds
                                                                                     
    deploy 
        --help # subcommand help
        --env # env defined in local.yaml
        --dry-run # Whether to do a dry-run, do dry-run if present
        --drop # drop all resources or resource specified with --include
        --drop-data # drop all data
        --include # include resource to deploy

```

## Challenges: 

**1. not intuitive when mixing global and subcommand flags:**



> Proposal: 
> From 
> *cdf-tk --verbose deploy --env prod --include assets*
> To
> *cdf-tk deploy --env prod --include assets --verbose*

```






# Proposal

init
auth
build
plan
apply
drop


    drop/drop-data interactive by default, quiet mode with flag