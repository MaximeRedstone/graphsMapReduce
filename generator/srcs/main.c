#include "graph_generator.h"
#include <unistd.h>
#include <stdlib.h>
#include <time.h>

int main(int argc, char **argv)
{
	int					error_code;
	t_generator			generator;
	time_t				end;

	generator.start = time(0);
	printf("Start program at %s", ctime(&(generator.start)));
	ft_memset(&generator, 0, sizeof(t_generator));
	if ((error_code = parse_user_args(argc, argv, &generator)) != EXIT_SUCCESS)
		return exit_failure(error_code);
	printf("Has parsed user argument\n");
	if ((error_code = convert_user_input(&generator)) != EXIT_SUCCESS)
		return exit_failure(error_code);
	printf("Has converted user argument to bytes\n");
	if ((error_code = generate_graph(argv[2], &generator)) != EXIT_SUCCESS)
		return exit_failure(error_code);
	end = time(0);
	printf("End program at %s", ctime(&end));
	return (EXIT_SUCCESS);
}
