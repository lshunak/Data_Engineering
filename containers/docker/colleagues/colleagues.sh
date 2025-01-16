#!/bin/bash

# List of colleagues' names
colleagues=("Maor" "Omer" "Tomer" "Alexei" "Mickael")

# Infinite loop
while true; do
  for name in "${colleagues[@]}"; do
    echo "$name"
  done
done
