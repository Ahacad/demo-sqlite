myjql : myjql.c
	gcc -o myjql myjql.c
clean :
	rm -rf *.o myjql
cleandb :
	rm -rf *.db
cleanall : 
	rm -rf myjql *.db
debug : myjql.c
	gcc -g -o myjql myjql.c
