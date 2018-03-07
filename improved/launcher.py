import numpy as np
import sys
import random
import json
import pika
import time
import threading
import os
import re
from scoop import logger, futures, launcher, utils
from scoop.launcher import ScoopApp, Host
NUMBER_OF_GENERATIONS = 100
POPULATION_SIZE = 10
CHROMOSOME_SIZE = 4
KNAPSACK_SIZE = 30
NUM_OF_NEIGHBOURS = 2


class Collect(object):
    def __init__(self):
        self._objects = []

    @property
    def objects(self):
        return self._objects

    def append_object(self, obj):
        return self._objects.append(obj)

    def sort_objects(self):
        return sorted(self._objects, key=lambda x: x.fit, reverse=True)

    def size_of_col(self):
        return len(self._objects)


class Snt(object):
    def __init__(self, fit, chromosome):
        self._fit = fit
        self._chromosome = chromosome

    @property
    def fit(self):
        return self._fit

    @property
    def chromosome(self):
        return self._chromosome

    def __str__(self):
        return "Fitness is " + str(self._fit) + " chromosome is " + str(self.chromosome)

    def __repr__(self):
        return self.__str__()


class Item(object):
    def __init__(self, volume, weight, name):
        self._weight = weight
        self._volume = volume
        self._name = name

    @property
    def volume(self):
        return self._volume

    @property
    def weight(self):
        return self._weight

    @property
    def name(self):
        return self._name


items = {0: Item(10, 30, 'knife'),
         1: Item(20, 40, 'food'),
         2: Item(5, 5, 'rope'),
         3: Item(10, 15, 'matches')}


def initialize_population():
    """
    Generate the population
    :return: population
    """
    population = []
    for i in range(0, POPULATION_SIZE):
        population.append(gen_individual())
    return population


def gen_individual():
    """
    Generate binary array
    :return: individual's chromosome
    """
    return np.random.randint(2, size=CHROMOSOME_SIZE)


def crossover(father, mother):
    """
    Exchange the random number of bits
    between father and mother
    :param father
    :param mother
    """
    cross = random.randint(0, CHROMOSOME_SIZE - 1)
    for i in range(0, cross):
        mother[i] = father[i]
    for i in range(cross, CHROMOSOME_SIZE):
        father[i] = mother[i]


def mutation(chromosome):
    """
    Invert one random bit based on probability
    :param chromosome
    """
    if np.random.choice([True, False], p=[0.1, 0.9]):
        rnd = random.randint(0, CHROMOSOME_SIZE - 1)
        chromosome[rnd] = abs(chromosome[rnd] - 1)


def fitness(chromosome):
    """
    Fitness is processed as sum of weights.
    If volume is bigger than limit, fitness is zero
    :param chromosome
    :return: fitness value
    """
    # time.sleep(3)
    logger.info("Processing fitness function DONE: " + str(chromosome))
    fitness_value = 0
    volume_value = 0
    for i in range(0, CHROMOSOME_SIZE):
        fitness_value = fitness_value + chromosome[i] * items.get(i).weight
        volume_value = volume_value + chromosome[i] * items.get(i).volume
    return fitness_value if volume_value <= KNAPSACK_SIZE else 0


def find_solution(population):
    """
    Find the best solution
    :param population
    :return: best_weight, chromosome
    """
    max_val = 0
    max_index = None
    for i in range(0, POPULATION_SIZE):
        curr_fit = fitness(population[i])
        if curr_fit > max_val:
            max_val = curr_fit
            max_index = i
    return max_val, population[max_index]


def process(chromosome, channels):
    queue_to_produce = str(channels.pop(0))
    queues_to_consume = list(map(str, channels))
    logger.info("starting processing to queue: " + queue_to_produce
                + " and consuming from: " + str(queues_to_consume) + " with chromosome: "
                + str(chromosome))
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='127.0.0.1',
                                  credentials=pika.PlainCredentials("genetic1", "genetic1")))

    channel = connection.channel()

    channel.exchange_declare(exchange='direct_logs',
                             exchange_type='direct')
    channel.basic_qos(prefetch_count=len(queues_to_consume))

    result = channel.queue_declare(exclusive=True)
    queue_name = result.method.queue

    for queue in queues_to_consume:
        channel.queue_bind(exchange='direct_logs',
                           queue=queue_name,
                           routing_key=queue)
    time.sleep(5)

    for i in range(0, NUMBER_OF_GENERATIONS):
        fit = fitness(chromosome)
        to_send = [int(fit)]
        to_send.extend(list(map(int, chromosome)))
        channel.basic_publish(exchange='direct_logs',
                              routing_key=queue_to_produce,
                              body=json.dumps(to_send))
        logger.info(' [*] Waiting for logs')
        neighbours = Collect()
        while neighbours.size_of_col() != NUM_OF_NEIGHBOURS:
            method_frame, header_frame, body = channel.basic_get(queue=str(queue_name), no_ack=False)
            if body:
                received = list(map(int, json.loads(body)))
                logger.info(queue_to_produce + " RECEIVED " + str(received))

                fit_val = received.pop(0)
                vector = received
                print("PARSED " + str(fit_val) + " " + str(vector))
                neighbours.append_object(Snt(fit_val, vector))
                channel.basic_ack(method_frame.delivery_tag)

            else:
                logger.info(queue_to_produce + ' No message returned')

        sorted_x = neighbours.sort_objects()
        print("SORTED " + str(sorted_x))
        mother = sorted_x.pop(0).chromosome
        logger.info("father " + str(chromosome) + " mother " + str(mother))
        crossover(chromosome, mother)
        # mutate
        mutation(chromosome)

    fit = fitness(chromosome)
    logger.info("RETURN chromosome " + str(chromosome) + " with fitness " + str(fit))
    connection.close()
    return fit, chromosome


def initialize_topology(quantity, radius):
    channels_to_return = []
    for x in range(quantity):
        channels = [x]
        for z in range(1, radius + 1):
            if x+z > quantity - 1:
                channels.append(abs(quantity-(x+z)))
            else:
                channels.append(x+z)
            if x-z < 0:
                channels.append(abs((x-z) + quantity))
            else:
                channels.append(x-z)
        channels_to_return.append(channels)
    return channels_to_return


def parallel(hosts_list, num_of_workers):
    # Get a list of resources to launch worker(s) on
    hosts = utils.getHosts(None, hosts_list)
    external_hostname = [utils.externalHostname(hosts)]
    # Launch SCOOP
    print(sys.executable)
    thisScoopApp = ScoopApp(hosts=hosts, n=num_of_workers, b=1,
                            verbose=4,
                            python_executable=[sys.executable],
                            externalHostname=external_hostname[0],
                            executable="toRun.py",
                            arguments=None,
                            tunnel=False,
                            path="/home/martin/PycharmProjects/fineGrained/main/",
                            debug=False,
                            nice=None,
                            env=utils.getEnv(),
                            profile=None,
                            pythonPath=None,
                            prolog=None, backend='ZMQ')

    rootTaskExitCode = False
    interruptPreventer = threading.Thread(target=thisScoopApp.close)
    try:
        rootTaskExitCode = thisScoopApp.run()
    except Exception as e:
        logger.error('Error while launching SCOOP subprocesses:' + str(e))
        rootTaskExitCode = -1
    finally:
        # This should not be interrupted (ie. by a KeyboadInterrupt)
        # The only cross-platform way to do it I found was by using a thread.
        interruptPreventer.start()
        interruptPreventer.join()

    # Exit with the proper exit code
    if rootTaskExitCode:
        sys.exit(rootTaskExitCode)
