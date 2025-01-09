@echo off
REM Pull the Docker image
docker pull ghcr.io/dbt-labs/dbt-redshift:latest

REM Run the Docker container
docker run ^
--name cmmc ^
--rm ^
--network=host ^
--mount type=bind,source=./cmmc/,target=/usr/app ^
--mount type=bind,source=%cd%\cmmc\profiles.yml,target=/root/.dbt/profiles.yml ^
ghcr.io/dbt-labs/dbt-redshift:latest ^
ls

REM Output a message
echo Doc string blabla

REM Add aliases to the .bash_profile or .bashrc equivalent (PowerShell environment in Windows)
echo alias dbt-run="docker run --name cmmc --rm --network=host --mount type=bind,source=./cmmc/,target=/usr/app --mount type=bind,source=%cd%\cmmc\profiles.yml,target=/root/.dbt/profiles.yml ghcr.io/dbt-labs/dbt-redshift:latest dbt run" >> %USERPROFILE%\.bash_profile
echo alias dbt-test="docker run --name cmmc --rm --network=host --mount type=bind,source=./cmmc/,target=/usr/app --mount type=bind,source=%cd%\cmmc\profiles.yml,target=/root/.dbt/profiles.yml ghcr.io/dbt-labs/dbt-redshift:latest dbt test" >> %USERPROFILE%\.bash_profile
echo alias dbt-build="docker run --name cmmc --rm --network=host --mount type=bind,source=./cmmc/,target=/usr/app --mount type=bind,source=%cd%\cmmc\profiles.yml,target=/root/.dbt/profiles.yml ghcr.io/dbt-labs/dbt-redshift:latest dbt build" >> %USERPROFILE%\.bash_profile
REM Add other aliases as needed, following the same structure

REM Reload the environment to apply aliases
powershell -Command "Set-ExecutionPolicy RemoteSigned -Scope CurrentUser"
powershell -Command "Refresh-Env"
