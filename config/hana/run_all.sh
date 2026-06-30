echo "Starting SQLStorm"
java -jar benchbase.jar -b sqlstorm -c config/hana/sqlstorm.xml --create=true --load=true --execute=true >> ./sqlstorm_trino.txt