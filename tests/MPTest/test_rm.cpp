#include <iostream>
#include <pthread.h>
#include <stdlib.h>
#include <string>
#include <cstdio>
#include "time.h"
using namespace std;

static clock_t start, finish;
static int *result;

void* say_hello(void* args)
{
    string str("rm -fR /media/gridfs/");
    char c[10];
    sprintf(c,"%d",*((int *)args));
    str = str + c;
    //cout << str << endl;

    if(system(str.c_str())==0){
		clock_t finish_t = clock();
		if(finish_t>finish){
			finish = finish_t;
		}
		result[*((int *)args)] = 1;
		cout<<*((int *)args)<<endl;
	}else{
		cout<<"Error thread = "<<*((int *)args)<<endl;
	}

}

int main()
{
    int NUM_THREADS = 0;
    cout<<"Please input the amount of threads:"<<endl;
    cin>>NUM_THREADS;
    pthread_t tids[NUM_THREADS];
    int indexs[NUM_THREADS];
    string files[NUM_THREADS];
	result = new int[NUM_THREADS];

    double duration;
    start = clock();
	cout<<"***************************start: "<<start<<endl;
	finish = start;
    for(int i = 0; i < NUM_THREADS; ++i)
    {
        indexs[i] = i; 
        int ret = pthread_create( &tids[i], NULL, say_hello, (void *)&(indexs[i]) );
        if (ret != 0)
        {
           cout << "pthread_create error: error_code=" << ret << endl;
        }

		pthread_join(tids[i],NULL);
    }
	cout<<"***************************finish: "<<finish<<endl;
	cout<<"延时20s等待所有线程结束！！！"<<endl;
	sleep(20);

    duration = (double)(finish - start)/ CLOCKS_PER_SEC;
    cout<<"RPS = "<<(NUM_THREADS/duration)<<endl;
	int j;
	int sum = 0;
	for(j=0;j<NUM_THREADS;j++){
		sum = sum + result[j];
	}
	cout<<"Error number of threads = "<<(NUM_THREADS-sum)<<endl;
	cout<<"Error Ratio = "<<((double)(NUM_THREADS-sum)/(double)NUM_THREADS)<<endl;
}
