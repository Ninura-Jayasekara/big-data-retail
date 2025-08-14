#!/bin/bash
spark-submit scripts/01_clean_transform.py
spark-submit scripts/02_rfm.py
spark-submit scripts/03_fpgrowth.py
