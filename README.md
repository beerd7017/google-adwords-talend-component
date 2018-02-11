# Welcome to the Google Adwords Talend Component Repository

**Warning! This component has not yet been fully tested or completed as of 2/11/2018. Alpha release is planned to be ready by 2/23/2018.**

## Purpose
This repository contains the source code for a Talend Component to make integration with Google Adwords easy. 

## Rules of the Repository
* Master branch is production, period.
* Do not commit to master, please work off the development branch.
* When working locally, try to work in feature branches.
* Follow your git flow in `SourceTree`.

## Build Script
The following command will being a build and database migration for the local instance. To target another instance modify the `-Penv` flag. 

`build -Penv=local flywayMigrate -i --stacktrace`
