def write_output(percentage,output_file_name):
    with open(output_file_name, "a") as f:
        for x, y in percentage:
            f.write(str(x) + "," + str(y) + "\n")

def write_duration(p, times, duration_file_name):
    with open(duration_file_name, "a") as f:
        f.write("Parameter p-->" + str(p) + "\n")
        f.write(str(times[0]) + "\n")
        f.write(str(times[1]) + "\n")
        f.write(str(times[2]) + "\n")
        f.write("------ end execution ------\n")