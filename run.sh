jstorm kill 424452my9i
git pull
mvn clean assembly:assembly -Dmaven.test.skip=true
cd target
jstorm jar jstorm jar singularity.jar com.alibaba.middleware.race.jstorm.RaceTopology --exclude-jars slf4j-log4j
