from celery import Celery

app = Celery('tasks', backend='amqp://localhost', broker='amqp://localhost')

###
# sample task
###
@app.task
def generate_prime(val):
    multiples = []
    result = []
    for i in xrange(2, val+1):
        if i not in multiples:
            result.append(i)
            for j in xrange(i*i, val+1, i):
                multiples.append(j)
    return result