# Meltano ELT with Prefect

This project contains a meltano load specification and a prefect flow that runs it.

The meltano task is run via the `meltano elt <tap-name> <target-name>` command, in this case the source is an sftp-server and the target is a snowflake database, so we run `meltano elt tap-sftp target-snowflake`. We use a Prefect Shell task for this.

The meltano config file does not contain any sensitive information like passwords. Meltano can read this from predefined environment variables, which store in prefect secrets and pass into the shell task at runtime through the `env` argument.

Because tasks does not preserve state, we need to store the information about what has already been loaded (high water mark) in a separate location. Meltano supports using postgres databases for this, and we pass in a postgres connection string as another environment variable.

The Prefect Secrets look as follow:

**MELTANO_DATABASE_URI**
`postgresql://<username>:<password>@<host>:5432/<database-name>`

**TAP_SFTP_CONFIG**
```
{
    "TAP_SFTP_PASSWORD": "super-secret-password-1",
    "TAP_SFTP_USERNAME": "<my-sftp-user>",
}
```

**TARGET_SNOWFLAKE_CONFIG**
```
{
    "account": "<xy12345.us-east-1.aws>",
    "user": "<snowflakeuser>",
    "password": "<super-secret-password-2>"
}
```
