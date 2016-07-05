#!/bin/bash
cd ../target
jstorm jar singularity.jar com.alibaba.middleware.race.jstorm.RaceTopology --exclude-jars slf4j-log4j
