#!/usr/bin/env bash

set -ex

BUILD_DIR="/build"
RESULT_DIR="/build/artifacts"
CNAME="ton-node"

#----------------------------------------
# bring cmake

cd /usr/src
version=3.13
build=5
mkdir ~/temp
cd ~/temp
curl -s -L -o cmake-$version.$build-Linux-x86_64.sh https://cmake.org/files/v$version/cmake-$version.$build-Linux-x86_64.sh
mkdir /usr/local/cmake
/bin/bash cmake-$version.$build-Linux-x86_64.sh --prefix=/usr/local/cmake --skip-license
ln -s /usr/local/cmake/bin/cmake /usr/local/bin/cmake

#----------------------------------------
# get sources

cd $BUILD_DIR
git submodule update --init --recursive
git checkout ${VERSION}

#----------------------------------------
# build

cmake -version
gcc -v

cd ${BUILD_DIR}
mkdir build
cd build
cmake   -DCMAKE_VERBOSE_MAKEFILE:BOOL=ON \
        -L \
        -DCMAKE_BUILD_TYPE=RelWithDebInfo \
        ..
cmake --build . --target validator-engine -- -j 4
cmake --build . --target validator-engine-console -- -j 4
cmake --build . --target blocks-stream-reader -- -j 4
cmake --build . --target generate-random-id -- -j 4
cmake --build . --target lite-client -- -j 4

#----------------------------------------
# create artifact

if [ "$VERSION" == "master" ]; then
    #ARTIFACT_NAME="${CNAME}-${VERSION}"-$(date '+%Y%m%d%H%M')
    ARTIFACT_NAME="${CNAME}-${SHA256SUM}"
else
    ARTIFACT_NAME="${CNAME}-${VERSION}"
fi

ARTIFACT_DIR=${RESULT_DIR}/${ARTIFACT_NAME}
mkdir -p ${ARTIFACT_DIR}/bin

cp -v ${BUILD_DIR}/build/validator-engine/validator-engine                    ${ARTIFACT_DIR}/bin/
cp -v ${BUILD_DIR}/build/validator-engine-console/validator-engine-console    ${ARTIFACT_DIR}/bin/
cp -v ${BUILD_DIR}/build/lite-client/lite-client                              ${ARTIFACT_DIR}/bin/
cp -v ${BUILD_DIR}/build/utils/generate-random-id                             ${ARTIFACT_DIR}/bin/
cp -v ${BUILD_DIR}/build/blocks-stream/blocks-stream-reader                   ${ARTIFACT_DIR}/bin/
#strip ${ARTIFACT_DIR}/bin/*

cd $RESULT_DIR
tar -vczf ${ARTIFACT_NAME}-linux64.tar.gz *
echo VERSION=${VERSION} >> $RESULT_DIR/VERSION.txt
