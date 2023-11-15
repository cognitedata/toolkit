#!/bin/bash
echo "Running copy commands to prep deployment of demo..."
pushd `dirname $0`
echo "Copying my local.yaml to root of repo..."
cp local.yaml ../cognite_toolkit/
echo "Copying config.yaml into cdf_auth_readwrite_all module..."
cp config.cdf_auth_readwrite_all.yaml ../cognite_toolkit/common/cdf_auth_readwrite_all/config.yaml
echo "Copying config.yaml into cdf_infield_common module..."
cp config.cdf_infield_common.yaml ../cognite_toolkit/common/cdf_infield_common/config.yaml

popd