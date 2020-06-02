#!/bin/bash

javac -cp jBCrypt-0.4/jbcrypt.jar Calibrator.java
java -cp .:jBCrypt-0.4/jbcrypt.jar Calibrator | tee calibration.txt
