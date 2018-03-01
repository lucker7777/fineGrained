import numpy as np
import operator
import random
import json
import pika
from scoop import futures, logger

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
    return np.random.randint(2, size=(CHROMOSOME_SIZE))


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
        if (curr_fit > max_val):
            max_val = curr_fit
            max_index = i
    return max_val, population[max_index]


def process1(bla):
    queue_to_produce = bla[0]
    queues_to_consume = bla[1]
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

    popula = initialize_population()

    for i in range(0, NUMBER_OF_GENERATIONS):
        solution, sol_vec = find_solution(popula)
        channel.basic_publish(exchange='direct_logs',
                              routing_key=queue_to_produce,
                              body=json.dumps(list(map(int, sol_vec))))
        logger.info(' [*] Waiting for logs')
        method_frame = None
        body = None
        while method_frame is None:
            method_frame, header_frame, body = channel.basic_get(queue=str(queue_name), no_ack=False)
            if method_frame:
                logger.info("BLAA " + str(body))
                channel.basic_ack(method_frame.delivery_tag)
            else:
                logger.info('No message returned')
        # put best individuals
        individual = []
        individual.append(json.loads(body))
        for i in range(0, len(individual)):
            logger.info("Received: " + str(individual[i]))
            popula[i] = individual[i]
        logger.info("actual: weight: " + str(solution) + " vector: " + str(sol_vec))
    solution, sol_vec = find_solution(popula)
    logger.info("FINAL RESULT: weight: " + str(solution) + " vector: " + str(sol_vec))
    connection.close()
    return sol_vec


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
            if body:
                logger.info(queue_to_produce + " RECEIVED " + str(body))
                received = list(map(int, chromosome))
                fit_val = received.pop(0)
                vector = received
                neighbours[fit_val] = vector
                channel.basic_ack(method_frame.delivery_tag)

            else:
                logger.info(queue_to_produce + ' No message returned')
        sorted_x = sorted(neighbours.items(), key=operator.itemgetter(0))
        mother = sorted_x.pop(0)
        logger.info("father " + str(chromosome) + " mother " + str(mother))
        crossover(chromosome, mother)
        # mutate
        mutation(chromosome)

    fit = fitness(chromosome)
    logger.info("RETURN chromosome " + str(chromosome) + " with fitness " + str(fit))
    connection.close()
    return fit, chromosome


if __name__ == '__main__':
    arr = []
    one = ["1", "2", "3"]
    two = ["2", "1", "3"]
    three = ["3", "1", "2"]
    arr.append(one)
    arr.append(two)
    arr.append(three)

    popula = initialize_population()

    logger.info("END " + str(list(futures.map(process, popula, arr))))
