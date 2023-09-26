#!/bin/zsh
for ((i = 0; i < 20; i++)); do 
    go test -race -run 2A;
done;