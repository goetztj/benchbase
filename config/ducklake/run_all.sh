echo "Starting YCSB A"
java -jar benchbase.jar -b ycsb -c config/ducklake/ycsb_config_a1.xml --create=true --load=true --execute=true >> ./ycsb_a_ducklake_1.txt
java -jar benchbase.jar -b ycsb -c config/ducklake/ycsb_config_a2.xml --create=true --load=true --execute=true >> ./ycsb_a_ducklake_2.txt
java -jar benchbase.jar -b ycsb -c config/ducklake/ycsb_config_a4.xml --create=true --load=true --execute=true >> ./ycsb_a_ducklake_4.txt
java -jar benchbase.jar -b ycsb -c config/ducklake/ycsb_config_a8.xml --create=true --load=true --execute=true >> ./ycsb_a_ducklake_8.txt
java -jar benchbase.jar -b ycsb -c config/ducklake/ycsb_config_a16.xml --create=true --load=true --execute=true >> ./ycsb_a_ducklake_16.txt
java -jar benchbase.jar -b ycsb -c config/ducklake/ycsb_config_a32.xml --create=true --load=true --execute=true >> ./ycsb_a_ducklake_32.txt
java -jar benchbase.jar -b ycsb -c config/ducklake/ycsb_config_a64.xml --create=true --load=true --execute=true >> ./ycsb_a_ducklake_64.txt

echo "Starting YCSB B"
java -jar benchbase.jar -b ycsb -c config/ducklake/ycsb_config_b1.xml --create=true --load=true --execute=true >> ./ycsb_b_ducklake_1.txt
java -jar benchbase.jar -b ycsb -c config/ducklake/ycsb_config_b2.xml --create=true --load=true --execute=true >> ./ycsb_b_ducklake_2.txt
java -jar benchbase.jar -b ycsb -c config/ducklake/ycsb_config_b4.xml --create=true --load=true --execute=true >> ./ycsb_b_ducklake_4.txt
java -jar benchbase.jar -b ycsb -c config/ducklake/ycsb_config_b8.xml --create=true --load=true --execute=true >> ./ycsb_b_ducklake_8.txt
java -jar benchbase.jar -b ycsb -c config/ducklake/ycsb_config_b16.xml --create=true --load=true --execute=true >> ./ycsb_b_ducklake_16.txt
java -jar benchbase.jar -b ycsb -c config/ducklake/ycsb_config_b32.xml --create=true --load=true --execute=true >> ./ycsb_b_ducklake_32.txt
java -jar benchbase.jar -b ycsb -c config/ducklake/ycsb_config_b64.xml --create=true --load=true --execute=true >> ./ycsb_b_ducklake_64.txt

echo "Starting YCSB C"
java -jar benchbase.jar -b ycsb -c config/ducklake/ycsb_config_c1.xml --create=true --load=true --execute=true >> ./ycsb_c_ducklake_1.txt
java -jar benchbase.jar -b ycsb -c config/ducklake/ycsb_config_c2.xml --create=true --load=true --execute=true >> ./ycsb_c_ducklake_2.txt
java -jar benchbase.jar -b ycsb -c config/ducklake/ycsb_config_c4.xml --create=true --load=true --execute=true >> ./ycsb_c_ducklake_4.txt
java -jar benchbase.jar -b ycsb -c config/ducklake/ycsb_config_c8.xml --create=true --load=true --execute=true >> ./ycsb_c_ducklake_8.txt
java -jar benchbase.jar -b ycsb -c config/ducklake/ycsb_config_c16.xml --create=true --load=true --execute=true >> ./ycsb_c_ducklake_16.txt
java -jar benchbase.jar -b ycsb -c config/ducklake/ycsb_config_c32.xml --create=true --load=true --execute=true >> ./ycsb_c_ducklake_32.txt
java -jar benchbase.jar -b ycsb -c config/ducklake/ycsb_config_c64.xml --create=true --load=true --execute=true >> ./ycsb_c_ducklake_64.txt

echo "Starting TPC-C 1"
java -jar benchbase.jar -b tpcc -c config/ducklake/tpcc_config_1.xml  --create=true  --load=true  --execute=true >> ./tpcc_dl_1h_1c.txt

#echo "Starting TPC-C 2 limited"
#java -jar benchbase.jar -b tpcc -c config/ducklake/tpcc_config_2_limited.xml  --create=true  --load=true  --execute=true >> ./tpcc_dl_1h_2c_lim.txt

#echo "Starting TPC-C 2"
#java -jar benchbase.jar -b tpcc -c config/ducklake/tpcc_config_2.xml  --create=true  --load=true  --execute=true >> ./tpcc_dl_1h_2c_unlim.txt

#echo "Starting TPC-C 4 limited"
#java -jar benchbase.jar -b tpcc -c config/ducklake/tpcc_config_4_limited.xml  --create=true  --load=true  --execute=true >> ./tpcc_dl_1h_4c_lim.txt

echo "Starting TPC-C 4"
java -jar benchbase.jar -b tpcc -c config/ducklake/tpcc_config_4.xml  --create=true  --load=true  --execute=true >> ./tpcc_dl_1h_4c_unlim.txt