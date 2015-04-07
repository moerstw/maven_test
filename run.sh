rm ./output -r;
yarn jar ./target/maven_test-1.0-SNAPSHOT.jar input output 1;
hdfs dfs -get output ./ ;
