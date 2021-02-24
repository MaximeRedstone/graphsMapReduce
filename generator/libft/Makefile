# **************************************************************************** #
#                                                                              #
#                                                         :::      ::::::::    #
#    Makefile                                           :+:      :+:    :+:    #
#                                                     +:+ +:+         +:+      #
#    By: sikpenou <marvin@42.fr>                    +#+  +:+       +#+         #
#                                                 +#+#+#+#+#+   +#+            #
#    Created: 2019/04/02 21:32:05 by sikpenou          #+#    #+#              #
#    Updated: 2020/01/13 10:34:34 by sikpenou         ###   ########.fr        #
#                                                                              #
# **************************************************************************** #

CC = gcc

CFLAGS = -Wall -Wextra -Werror -g

NAME = libft.a

SRCS = $(addprefix $(SRCDIR), $(SRCFILES))

SRCFILES = \
			filler_gnl.c \
			ft_abs.c \
			ft_atoi.c \
			ft_atoi_base.c \
			ft_atou_mv.c \
			ft_check_base.c \
			ft_convert_base.c \
			ft_find_next_prime.c \
			ft_free.c \
			ft_free_tab.c \
			ft_isalnum.c \
			ft_isalpha.c \
			ft_isascii.c \
			ft_isdigit.c \
			ft_isprint.c \
			ft_isspace.c \
			ft_itoa.c \
			ft_itoa_base.c \
			ft_lstsort.c \
			ft_lstinsert.c \
			ft_lstcycle.c \
			ft_lstadd.c \
			ft_lstadd_sorted.c \
			ft_lstadd_back.c \
			ft_lstadd_back_new.c \
			ft_lstadd_new.c \
			ft_lstnew.c \
			ft_lstcpy.c \
			ft_lstnew_elem.c \
			ft_lstnew_head.c \
			ft_lstfree.c \
			ft_lstfree_elem.c \
			ft_lstfree_head.c \
			ft_lstjoin.c \
			ft_lstpop.c \
			ft_lstfind.c \
			ft_lstprint.c \
			ft_lstswap_heads.c \
			ft_lstswap_contents.c \
			ft_lst_replace_elem.c \
			ft_malloc.c \
			ft_max.c \
			ft_memalloc.c \
			ft_memccpy.c \
			ft_memchr.c \
			ft_memchr_pos.c \
			ft_memcmp.c \
			ft_memcpy.c \
			ft_memcset.c \
			ft_memdel.c \
			ft_memmove.c \
			ft_memset.c \
			ft_min.c \
			ft_pow.c \
			ft_printf_pf.c \
			ft_putchar.c \
			ft_putchar_fd.c \
			ft_putnbr.c \
			ft_putnbr_fd.c \
			ft_putstr.c \
			ft_putstr_fd.c \
			ft_putstr_fd_ret.c \
			ft_putstr_ret.c \
			ft_realloc.c \
			ft_strchr.c \
			ft_strchr_pos.c \
			ft_strcmp.c \
			ft_strcpy.c \
			ft_strdel.c \
			ft_strdup.c \
			ft_strjoin.c \
			ft_strjoin_free.c \
			ft_strlen.c \
			ft_strncmp.c \
			ft_strncpy.c \
			ft_strnew.c \
			ft_strrchr.c \
			ft_strrev.c \
			ft_strsplit.c \
			ft_strstr.c \
			ft_strsub.c \
			ft_strtrim.c \
			ft_swap.c \
			ft_swapstr.c \
			ft_tolower.c \
			ft_toupper.c \
			ft_ulltoa.c \
			core_pf.c \
			parsing_pf.c \
			num_funcs_pf.c \
			text_funcs_pf.c \
			padding_pf.c \
			support_pf.c \
			hash_tables_memory.c \
			hash_tables_access.c \
			hash_tables_insert.c \
			hash_tables_display.c

SRCDIR = srcs/

OBJS = $(addprefix $(OBJDIR), $(OBJFILES))

OBJFILES = $(SRCFILES:.c=.o)

OBJDIR = objs/

INCLS = $(INCLDIR)libft.h

INCLDIR = includes/

all: $(NAME)

$(NAME): $(INCLS) $(SRCS) Makefile
	@/bin/echo make $(NAME)
	@rm -f auteur
	@mkdir -p $(OBJDIR)
	@make -j --no-print-directory objects
	@ar sr $(NAME) $(OBJS)
	@ranlib $(NAME)

objects: $(OBJS)

$(OBJDIR)%.o: $(SRCDIR)%.c $(INCLS)
	@$(CC) $(CFLAGS) -I $(INCLDIR) -o $@ -c $<

clean: FORCE
	@rm -rf $(OBJDIR)
	@echo "rm -f .o files"

fclean: clean
	@rm -f a.out
	@rm -f test
	rm -f $(NAME)
	rm -f ft_printf

re: FORCE
	@make -j --no-print-directory fclean
	@make -j --no-print-directory all

FORCE:

#.SILENT:
