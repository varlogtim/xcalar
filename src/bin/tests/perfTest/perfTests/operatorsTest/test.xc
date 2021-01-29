load --url nfs:///netstore/datasets/yelp/user --format json --name basicTestsYelpUser --size 10737418240
load --url nfs:///netstore/datasets/yelp/reviews --format json --name basicTestsYelpReviews --size 10737418240

index --key user_id --dataset .XcalarDS.basicTestsYelpUser --dsttable \"basicTests/table1\" --prefix pyelpuser
index --key user_id --dataset .XcalarDS.basicTestsYelpReviews --dsttable \"basicTests/table2\" --prefix pyelpreviews
join --leftTable \"basicTests/table1\" --rightTable \"basicTests/table2\" --joinTable \"basicTests/joinTable\"

drop table \"basicTests/table1\"
drop table \"basicTests/table2\"
drop table \"basicTests/joinTable\"
