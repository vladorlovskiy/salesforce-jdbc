source ~/java8
export MAVEN_BASEDIR=
mvn clean package -P single-jar -Duser.timezone="GMT-08:00" && cp -f ./sf-jdbc-driver/target/sf-jdbc-driver-*-SNAPSHOT-jar-with-dependencies.jar ./deliverables/
