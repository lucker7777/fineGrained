import numpy as np
import operator
import random
import json
import pika
from scoop import logger

NUMBER_OF_GENERATIONS = 100
POPULATION_SIZE = 10
CHROMOSOME_SIZE = 4
KNAPSACK_SIZE = 30
NUM_OF_NEIGHBOURS = 2


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


def process(chromosome, channels):
    queue_to_produce = channels.pop()
    queues_to_consume = channels
    logger.info("starting processing to queue: " + str(queue_to_produce)
                + " and consuming from: " + str(queues_to_consume))
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='127.0.0.1',
                                  credentials=pika.PlainCredentials("genetic1", "genetic1")))

    channel = connection.channel()

    channel.exchange_declare(exchange='direct_logs',
                             exchange_type='direct')
    channel.basic_qos(prefetch_count=len(queues_to_consume))

    result = channel.queue_declare(exclusive=True)
    queue_name = result.method.queue

    channel.queue_bind(exchange='direct_logs',
                       queue=queue_name,
                       routing_key=queues_to_consume)

    for i in range(0, NUMBER_OF_GENERATIONS):
        fit = fitness(chromosome)
        to_send = [int(fit)]
        to_send.extend(list(map(int, chromosome)))
        channel.basic_publish(exchange='direct_logs',
                              routing_key=queue_to_produce,
                              body=json.dumps(to_send))
        logger.info(' [*] Waiting for logs')
        neighbours = {}
        while len(neighbours) != NUM_OF_NEIGHBOURS:
            method_frame, header_frame, body = channel.basic_get(queue=str(queue_name), no_ack=False)
            if method_frame:
                logger.info("BLAA " + str(body))
                received = list(map(int, chromosome))
                fit_val = received.pop()
                vector = received
                neighbours[fit_val] = vector
                channel.basic_ack(method_frame.delivery_tag)
            else:
                logger.info('No message returned')
        sorted_x = sorted(neighbours.items(), key=operator.itemgetter(0))
        mother = sorted_x.pop()
        logger.info("father " + str(chromosome) + " mother " + str(mother))
        crossover(chromosome, mother)
        # mutate
        mutation(chromosome)

    fit = fitness(chromosome)
    logger.info("RETURN chromosome " + str(chromosome) + " with fitness " + str(fit))
    connection.close()
    return fit, chromosome
