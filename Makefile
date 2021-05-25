myjql : myjql.c
	gcc -o myjql myjql.c && rm -rf *.db
clean :
	rm -rf *.o myjql
cleandb :
	rm -rf *.db
cleanall : 
	rm -rf myjql *.db *.out
debug : myjql.c
	gcc -g -o myjql myjql.c
