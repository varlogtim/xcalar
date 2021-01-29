#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
# Based on ASF's CMake module for Snappy
find_path(PYTHON_INCLUDE_DIRS
    NAMES Python.h
    HINTS "${CMAKE_INSTALL_PREFIX}/include/python3.6m/"
    NO_DEFAULT_PATH
)

find_library(PYTHON_LIBRARIES
    NAMES python3.6m libpython3.6m libpython3.6m.a
    HINTS "${CMAKE_INSTALL_PREFIX}/lib/"
    NO_DEFAULT_PATH
)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(PythonLib DEFAULT_MSG
    PYTHON_LIBRARIES
    PYTHON_INCLUDE_DIRS)

mark_as_advanced(
    PYTHON_ROOT_DIR
    PYTHON_LIBRARIES
    PYTHON_INCLUDE_DIRS)
