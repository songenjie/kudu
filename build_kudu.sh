#export http_proxy=http://fq.mioffice.cn:3128
#export https_proxy=$http_proxy
#export ftp_proxy=$http_proxy
#export rsync_proxy=$http_proxy
#export no_proxy="localhost,127.0.0.1,localaddress,.localdomain.com"

KUDU_VERSION=`cat version.txt`
OS=`lsb_release -d | awk '{print $2}'`
echo "Start to build kudu $KUDU_VERSION on $OS"

function package_release {
  echo "Starting package kudu"
  BASE_DIR=`pwd`/build/
  PACK_DIR=$BASE_DIR/kudu-$KUDU_VERSION
  rm -rf $PACK_DIR  $PACK_DIR.tar.gz
  mkdir -p $PACK_DIR
  echo "Created $PACK_DIR"
  echo "Moving files to $PACK_DIR"
  cp build/latest/bin/kudu-master $PACK_DIR/kudu_master
  cp build/latest/bin/kudu-tserver $PACK_DIR/kudu_tablet_server
  cp build/latest/bin/kudu $PACK_DIR/kudu
  cp -r www $PACK_DIR/
  cd $BASE_DIR
  tar czf kudu-$KUDU_VERSION.tar.gz kudu-$KUDU_VERSION
  echo "Packaged $PACK_DIR.tar.gz"
}

if [ "$OS" == "CentOS" ]; then
  build-support/enable_devtoolset.sh thirdparty/build-if-necessary.sh
elif [ "$OS" == "Ubuntu" ]; then
  thirdparty/build-if-necessary.sh
else
  echo "Unsupported OS: $OS in $0"
  exit
fi

mkdir -p build/release
cd build/release
../../thirdparty/installed/common/bin/cmake -DCMAKE_BUILD_TYPE=release ../..
make -j4
cd ../../

if [ $? -eq 0 ]; then
  echo "Build Kudu succeed"
  package_release 
  echo "Please install kudu package: package install kudu --package_dir=./build/ --version=$KUDU_VERSION"
else 
  echo "Build Kudu failed"
fi
