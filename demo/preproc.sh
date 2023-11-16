#!/bin/bash
echo "Running copy commands to prep deployment of demo..."
pushd `dirname $0`
echo "Copying my local.yaml to root of repo..."
cp local.yaml ../demo_project/
echo "Copying config.yaml into cdf_auth_readwrite_all module..."
cp config.cdf_auth_readwrite_all.yaml ../demo_project/common/cdf_auth_readwrite_all/config.yaml
echo "Copying config.yaml into cdf_infield_common module..."
cp config.cdf_infield_common.yaml ../demo_project/modules/cdf_infield_common/config.yaml

popd