#!/bin/bash

cd Cquirrel-Script
python data_generate.py
python data_merge.py

cd ../Cquirrel-Core

mvn clean package
time java --add-opens java.base/java.util=ALL-UNNAMED --add-opens java.base/java.lang=ALL-UNNAMED -jar target/Cquirrel-Core-1.0-SNAPSHOT.jar > output.txt

cd ../Cquirrel-Script
time python data_check.py > check.txt