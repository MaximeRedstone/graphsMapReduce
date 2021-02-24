CC = gcc

CFLAGS = -Wall -Wextra -Werror -g3 #-fsanitize=address

NAME = graph_generator

SRCS = $(addprefix $(SRC_DIR), $(SRC_FILES))

SRC_FILES =	main.c	parser.c	generator.c	generate_edge.c	error.c	write_header.c

SRC_DIR = ./srcs/

OBJS = $(addprefix $(OBJ_DIR), $(OBJ_FILES))

OBJ_FILES = $(SRC_FILES:.c=.o)

OBJ_DIR = ./objs/

INCLS = $(INC_DIR)graph_generator.h

INC_DIR = includes/

LIB = $(addprefix $(LIB_DIR), libft.a)

LIB_INCLS = $(LIB_DIR)$(INC_DIR)

LIB_DIR = libft/

all: $(NAME)

$(NAME): $(INCLS) $(LIB_INCLS) $(SRCS) $(LIB) Makefile
	@/bin/echo "make $(notdir $(NAME))"
	@mkdir -p objs
	@make -j --no-print-directory objects
	@$(CC) $(CFLAGS) -I $(INC_DIR) -o $@ $(OBJS) $(LIB)

$(LIB): FORCE
	@make -j --no-print-directory -C $(LIB_DIR)

objects: $(OBJS)

$(OBJ_DIR)%.o: $(SRC_DIR)%.c $(INCLS)
	@$(CC) $(CFLAGS) -I $(INC_DIR) -I $(LIB_INCLS) -o $@ -c $<

clean: FORCE
	@/bin/echo "make clean $(notdir $(NAME))"
	@rm -rf $(OBJ_DIR)
	@make clean -j --no-print-directory -C $(LIB_DIR)

fclean: clean
	@/bin/echo "make fclean $(notdir $(NAME))"
	@make fclean -j --no-print-directory -C $(LIB_DIR)
	@rm -f $(NAME)


re: FORCE
	@/bin/echo "make re $(notdir $(NAME))"
	@make -j --no-print-directory fclean
	@make -j --no-print-directory all

FORCE:

#.SILENT:
