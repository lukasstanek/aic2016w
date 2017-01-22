#!/usr/bin/env bash



function startup{
    docker-compose up -d

    sleep 10;

    /home/lukas/repos/tuwien/venv/bin/python /home/lukas/repos/tuwien/aic/G4T1/tDatasource/app.py


}