#!/bin/bash
cd ~/Documentos
tar -zxvf dados-meteorologicos.tar.gz
mkdir dados-meteorologicos-extraidos
cd dados-meteorologicos
for i in $( ls -d */ ); do
	mkdir ~/Documentos/dados-meteorologicos-extraidos/$i
	cd $i
	for l in $( ls ); do
		tar -xvf $l
		rm $l
	done
	for j in $( ls ); do
		gunzip $j
		mv *.op ~/Documentos/dados-meteorologicos-extraidos/$i
	done
	cd ~/Documentos/dados-meteorologicos
done
