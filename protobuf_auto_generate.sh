#!/usr/bin/env bash
# 根据protobuf描述文件自动生成代码
protoc --java_out=$PROJECT_HOME/src/main/java  .protoexampleProtos

