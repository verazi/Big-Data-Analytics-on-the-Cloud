# COMP90024 Team 75 
# Linying Wei (1638206)
# Wangyang Wu (1641248)
# Ziyu Wang (1560831)
# Roger Zhang (1079986)

#!/bin/sh
pip3 install -r ${SRC_PKG}/requirements.txt -t ${SRC_PKG} && cp -r ${SRC_PKG} ${DEPLOY_PKG}