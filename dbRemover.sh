#!/bin/bash
systemctl stop neo4j
rm /var/lib/neo4j/data/databases/neo4j/* -rf
rm /var/lib/neo4j/data/transactions/neo4j/*
systemctl start neo4j
