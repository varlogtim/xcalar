# Instructions to run the test rig:

* The code execution is divided into 2 parts

## 1) Data lake creation part:

    * Data distribution stats came from Customer4 running profiler on their tables are located at
        "/netstore/datasets/customer4/pwm_pilot"

    * Test uses this stats to generate data and if not accessible will generate random data.

    * Data gen utility generates the first batch as base tables to speficied path with specified data target.

    * From second batch, data generates to IMD tables and can be written to disk or kafka server based on specified data targets and kafka server as inputs to the program.

    Example run:
        a) If remote cluster, run
            python dataGen/data_gen.py -H xdp-venu.westus2.cloudapp.azure.com -U xdpadmin -P Welcome1
        b) Check all available options as
            python dataGen/data_gen.py --help

## 2) Running test rig -> which loads data from data lake above and runs the refiner cycles

    * Test rig runs by creating apps on remote xcalar cluster per premise. App input should be provided as input.

    * Here is the path for app-config input to the program
        /netstore/datasets/customer4/simulationRig/appConfig/

    Example run:
        a) To run simulation rig on a remote cluster,
            python test_rig_main.py -H edison3.int.xcalar.com -a 8443 -U admin -P Welcome1 --app-config /netstore/datasets/customer4/simulationRig/appConfig/ --num-premises 1

        b) Check all available options as
            python test_rig_main.py --help

## ----------------------
## Notes
## ----------------------
https://xcalar.atlassian.net/wiki/spaces/FS/pages/68747278/How+to+Setup+Customer4-like+Solution
