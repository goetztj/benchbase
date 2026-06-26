echo "Starting YCSB A"
java -jar benchbase.jar -b ycsb -c config/trino/ycsb_config_a1.xml --create=true --load=true --execute=true >> ./ycsb_a_trino_1.txt