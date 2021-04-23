from ortools.linear_solver import pywraplp


def create_data_model():
    """Create the data for the example."""
    data = {}
    weights = [97,55,77,8,1,2,2,53,84,100,89,6,7,115,111,80,56,7,25,9,47,108,104,115,44,130,127,110,9,3,35,124,81,127,12,119,55,76,81,100,114,82,47,101,119,63,14,120,2,64,65,116,32,29,85,72,128,43,57,125,96,20,115,25,35,55,121,25,6,84,19,112,66,18,119,72,32,35,114,125,5,42,65,109,87,32,72,40,126,12,4,41,34,86,104,46,37,76,124,118,44,111,71,83,58,81,55,52,29,113,34,107,86,74,54,88,47,123,76,47,77,71,17,118,88,22,26,118,65,50,41,90,79,83,61,73,11,61,83,18,102,129,120,57,105,2,113,44,73,1,115,11,56,16,24,76,53,42,99,123,97,49,103,84,130,130,56,89,70,1,111,82,33,111,26,94,120,33,118,88,29,64,40,27,99,85,41,114,108,78,51,16,87,123,66,88,72,15,75,117,109,111,3,71,80,45,20,127,42,64,98,121,123,53,25,116,2,28,118,121,117,61,17,105,20,115,25,41,26,101,113,29,64,120,11,84,34,32,85,49,42,29,85,107,45,76,121,113,125,4,6,55,108,15,81,103,76,33,76,10,56,68,42,46,80,90,118,81,63,14,130,54,73,46,27,95,61,5,100,5,20,29,66,63,46,82,123,14,46,125,125,36,80,103,23,104,41,35,33,29,58,10,97,63,56,76,87,43,109,8,120,58,65,71,44,25,33,11,70,111,39,52,55,103,82,108,107,123,113,82,114,90,71,120,117,5,38,7,124,82,79,19,20,87,122,74,42,85,87,22,51,23,6,20,86,51,68,24,63,10,36,47,20,70,111,37,123,89,64,120,52,94,20,114,111,41,1,25,81,71,100,107,83,90,9,130,66,72,4,58,15,38,90,124,120,64,4,55,95,77,46,74,108,64,114,92,42,37,59,55,119,17,8,94,37,123,122,3,112,65,80,10,77,63,120,109,18,21,117,62,50,9,105,50,49,79,29,16,100,15,28,7,1,52,37,44,86,23,4,21,76,89,9,77,107,17,118,8,61,89,60,91,65,59,12,10,94,76,115,129,51,45,58,129,52,116,65,45,93,98,67,72,116,49,56,45,108,32,88,81,129,19,5,34,65,36,82,79,106,24]
    data['weights'] = weights
    data['items'] = list(range(len(weights)))
    data['bins'] = data['items']
    data['bin_capacity'] = 130
    return data



def main():
    data = create_data_model()

    # Create the mip solver with the SCIP backend.
    solver = pywraplp.Solver.CreateSolver('SCIP')


    # Variables
    # x[i, j] = 1 if item i is packed in bin j.
    x = {}
    for i in data['items']:
        for j in data['bins']:
            x[(i, j)] = solver.IntVar(0, 1, 'x_%i_%i' % (i, j))

    # y[j] = 1 if bin j is used.
    y = {}
    for j in data['bins']:
        y[j] = solver.IntVar(0, 1, 'y[%i]' % j)

    # Constraints
    # Each item must be in exactly one bin.
    for i in data['items']:
        solver.Add(sum(x[i, j] for j in data['bins']) == 1)

    # The amount packed in each bin cannot exceed its capacity.
    for j in data['bins']:
        solver.Add(
            sum(x[(i, j)] * data['weights'][i] for i in data['items']) <= y[j] *
            data['bin_capacity'])

    # Objective: minimize the number of bins used.
    solver.Minimize(solver.Sum([y[j] for j in data['bins']]))

    status = solver.Solve()

    if status == pywraplp.Solver.OPTIMAL:
        num_bins = 0.
        for j in data['bins']:
            if y[j].solution_value() == 1:
                bin_items = []
                bin_weight = 0
                for i in data['items']:
                    if x[i, j].solution_value() > 0:
                        bin_items.append(i)
                        bin_weight += data['weights'][i]
                if bin_weight > 0:
                    num_bins += 1
                    print('Bin number', j)
                    print('  Items packed:', bin_items)
                    print('  Total weight:', bin_weight)
                    print()
        print()
        print('Number of bins used:', num_bins)
        print('Time = ', solver.WallTime(), ' milliseconds')
    else:
        print('The problem does not have an optimal solution.')


if __name__ == '__main__':
    main()