import sys
import math

f = open(sys.argv[1])
clusternum_goal = int('0'+sys.argv[2])
line = f.readline()

input_data = []
while line:
    tmp = line.split(',')
    input_data.append(tmp)
    line = f.readline()

input_data.pop()

dataset = []
for i in range(len(input_data)):
    x = []
    for j in range(len(input_data[i])-1):
        x.append(float(input_data[i][j]))
    dataset.append(x)

dataset_list = []
for i in range(len(input_data)):
    x = []
    for j in range(len(input_data[i])-1):
        x.append(float(input_data[i][j]))
    dataset_list.append([x, input_data[i][4].strip()])

dataset_dict = {}
for i in range(len(dataset_list)):
    dataset_list[i][0] = tuple(dataset_list[i][0])
    dataset_list[i] = tuple(dataset_list[i])
dataset_dict = dict(dataset_list)

def Euclidean_distance(point_a, point_b):
    return math.sqrt(math.pow(point_a[0]-point_b[0], 2)+math.pow(point_a[1]-point_b[1], 2)+math.pow(point_a[2]-point_b[2], 2)+math.pow(point_a[3]-point_b[3], 2))

def distance_avg(cluster1, cluster2):
    return sum(Euclidean_distance(i, j) for i in cluster1 for j in cluster2)/(len(cluster1)*len(cluster2))

def find_mindistance(distance_matrix):
    x = 0
    y = 0    
    distance_min = 100#distance_matrix[0][1]
    for i in range(len(distance_matrix)):
        for j in range(len(distance_matrix[i])):
            if i != j and distance_matrix[i][j] < distance_min:
                distance_min = distance_matrix[i][j]
                x = i
                y = j
    return (x, y, distance_min)

def Hierarchical_Clustering(dataset, distance_func, clusternum_goal):
    cluster_matrix = []
    distance_matrix = []
    for i in dataset:
        tmp = []
        tmp.append(i)
        cluster_matrix.append(tmp)
#    return cluster_matrix
    for i in range(len(cluster_matrix)):
        tmp = []
        for j in range(len(cluster_matrix)):
            tmp.append(distance_func(cluster_matrix[i], cluster_matrix[j]))
        distance_matrix.append(tmp)
#    return distance_matrix
    clusternum_current = len(dataset)
    while clusternum_current > clusternum_goal:
        x, y, distance_min = find_mindistance(distance_matrix)
        distance_matrix = []
        cluster_matrix[x].extend(cluster_matrix[y])
        cluster_matrix.remove(cluster_matrix[y])
        for i in range(len(cluster_matrix)):
            tmp = []
            for j in range(len(cluster_matrix)):
                tmp.append(distance_func(cluster_matrix[i], cluster_matrix[j]))
            distance_matrix.append(tmp)
        clusternum_current -= 1
    return cluster_matrix

cluster_matrix = Hierarchical_Clustering(dataset, distance_avg, 3)

result_list = []

for i in range(len(cluster_matrix)):
    tmp = []
    for j in range(len(cluster_matrix[i])):
        tmp.append(cluster_matrix[i][j])
    result_list.append(tmp)

for i in range(len(cluster_matrix)):
    for j in range(len(cluster_matrix[i])):
        cluster_matrix[i][j] = tuple(cluster_matrix[i][j])

for i in range(len(cluster_matrix)):
    for j in range(len(cluster_matrix[i])):
        result_list[i][j].append(dataset_dict.get(cluster_matrix[i][j]))

result = []

for i in range(len(result_list)):
    num_Iris_setosa = 0
    num_Iris_versicolor = 0
    num_Iris_virginica = 0
    for j in range(len(result_list[i])):
        if result_list[i][j][4] == 'Iris-setosa': num_Iris_setosa +=1
        elif result_list[i][j][4] == 'Iris-versicolor': num_Iris_versicolor +=1
        elif result_list[i][j][4] == 'Iris-virginica': num_Iris_virginica +=1
    if num_Iris_setosa == max(num_Iris_setosa,num_Iris_versicolor,num_Iris_virginica):
        result.append(['Iris-setosa',result_list[i]])
    elif num_Iris_versicolor == max(num_Iris_setosa,num_Iris_versicolor,num_Iris_virginica):
        result.append(['Iris-versicolor',result_list[i]])
    elif num_Iris_virginica == max(num_Iris_setosa,num_Iris_versicolor,num_Iris_virginica):
        result.append(['Iris-virginica',result_list[i]])

num_cluster = []

for i in range(len(result)):
    num_cluster.append(len(result[i][1]))

num_wrong = 0
for i in range(len(result)):
    for j in range(len(result[i][1])):
        if result[i][1][j][4] != result[i][0]:
            num_wrong += 1

with open("Peng_Chenyu_cluster_"+str(clusternum_goal)+".txt","w") as f:
    for i in range(len(result)):
        f.write("cluster:"+result[i][0]+'\n')
        for j in range(len(result[i][1])):
            f.write(str(result[i][1][j])+'\n')
        f.write("Number of points in this cluster:"+str(num_cluster[i])+'\n\n')
    f.write("Number of points wrongly assigned:"+str(num_wrong))
'''
#            print(result[i][1][j])
            f.write('[')
            for k in range(len(result[i][1][j])):
                f.write(str(result[i][1][j][k]))
            f.write(']\n')'''
#print(cluster_matrix)
