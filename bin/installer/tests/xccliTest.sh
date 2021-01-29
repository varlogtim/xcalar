#!/bin/bash

/opt/xcalar/bin/xccli <<'EOF'
session --new --name new;
load --url memory:seq --format random --name s --size 100MB;
index -d .XcalarDS.s -t src -k key;
map --eval "float(key)" --s src -t 2 --fieldName k;
map --eval "float(value)" --s 2 -t 3 --fieldName v;
map --eval "string(key)" --s 3 -t 4 --fieldName strk;
map --eval "string(value)" --s 4 -t lrq --fieldName strv;

map --eval "add(key,value)" --s src -t fp --fieldName map;
map --eval "add(k,v)" --s lrq -t imm --fieldName map;
map --eval "concat(strk,strv)" --s lrq -t str --fieldName map;

index -s fp -t 5 -k map;
index -s imm -t 6 -k map;
index -s str -t 7 -k map;
groupBy --eval "count(key)" --srctable 5 -t gbFp --fieldName gb;
groupBy --eval "count(k)" --srctable 6 -t gbImm --fieldName gb;
groupBy --eval "count(strk)" --srctable 7 -t gbstr --fieldName gb;

index -s lrq -t left -k strk;
index -s lrq -t right -k strv;
join --leftTable left --rightTable right --joinTable join;
EOF
