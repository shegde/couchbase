#!/bin/bash

for (( c=1; c<=100; c++ ))
do
        python couchload.py &
done
