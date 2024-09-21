#!/bin/sh

echo "Started Job"

echo 'Env:'
echo npc = ${npc}
echo st = ${st}
echo NPROC = $NPROC

hostname

pwd

sleep 30

echo "Finished Job"

