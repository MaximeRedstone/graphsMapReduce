/* ************************************************************************** */
/*                                                                            */
/*                                                        :::      ::::::::   */
/*   libft.h                                            :+:      :+:    :+:   */
/*                                                    +:+ +:+         +:+     */
/*   By: sikpenou <marvin@42.fr>                    +#+  +:+       +#+        */
/*                                                +#+#+#+#+#+   +#+           */
/*   Created: 2019/10/01 18:42:52 by sikpenou          #+#    #+#             */
/*   Updated: 2020/01/13 15:19:09 by sikpenou         ###   ########.fr       */
/*                                                                            */
/* ************************************************************************** */

#ifndef LIBFT_H
# define LIBFT_H

# define EXIT 1
# define EASY 0
# define ADDR 0
# define NONE 0
# define BEFORE 0
# define AFTER 1
# define LEFT 1
# define RIGHT 2
# define BOTH 3
# define FREE_CONTENT 1
# define FREE_LINKS 2
# define FREE_BOTH 3
# define KEEP_HEAD 0
# define FREE_HEAD 1
# define BUFF_SIZE 40000
# define INITIAL_HASH_ARRAY_SIZE 4999
# define COLLISIONS_REJECTION_FACTOR 10
# define QUIET 0
# define VERBOSE 1
# define BLOCK 64
# define ALL "-+ 0#hlL.123456789*"
# define FLAGS "-+ 0#"
# define MINUS 1u
# define PLUS 2u
# define SPACE 4u
# define ZERO 8u
# define HASH 16u
# define FIELD "hhllL"
# define HFIELD 1u
# define HHFIELD 2u
# define LFIELD 3u
# define LLFIELD 4u
# define LMAJFIELD 5u
# define TYPE "\%cspduoxXb"
# define PERTYPE 0u
# define CTYPE 1u
# define STYPE 2u
# define PTYPE 3u
# define DTYPE 4u
# define UTYPE 5u
# define OTYPE 6u
# define XTYPE 7u
# define XMAJTYPE 8u
# define BTYPE 9u
# define ULLCHAIN "61615590737044764481"

# include <stdarg.h>
# include <stddef.h>
# include <stdio.h>

typedef struct			s_lst
{
	void				*content;
	struct s_lst		*prev;
	struct s_lst		*next;
}						t_lst;

typedef struct			s_head
{
	unsigned			size;
	t_lst				*first;
	t_lst				*last;
}						t_head;

typedef struct			s_h_elem
{
	unsigned			hash;
	char				*key;
	void				*content;
}						t_h_elem;

typedef int				(*t_h_iter_func)(t_h_elem *);

typedef unsigned		(*t_h_hash_func)(char *);

typedef void			(*t_h_free_func)(void **);

typedef struct			s_h_table
{
	unsigned			size;
	unsigned			elems;
	unsigned			collisions;
	unsigned			collisions_limit;
	t_h_hash_func		hash_func;
	t_h_free_func		free_func;
	t_head				*array;
}						t_h_table;

typedef struct			s_arg
{
	long				flags;
	long				min;
	long				prec;
	long				max;
	long				field;
	long				type;
	long				start;
	char				fill;
	char				*prefix;
}						t_arg;

typedef struct s_buf	t_buf;

typedef int				(*t_f)(t_arg *, t_buf *, va_list);

struct					s_buf
{
	char				*str;
	long				pos;
	long				lim;
	t_f					tab[7];
};

int						add_to_buf(char *str, t_buf *buf);
int						ft_printf(const char *str, ...);
int						ft_fprintf(int fd, const char *str, ...);
int						ft_sprintf(char **dest, const char *str, ...);
int						parse_arg(t_buf *buf, t_arg *arg, const char *str
	, va_list arg_list);
t_buf					*core_func(const char *str, va_list arg_list);
int						ft_realloc(void **zone, long curr_size
	, long to_add);
unsigned long long		ft_ato_ull(const char *str, int *pos);
int						ft_display(t_buf *buf, int opt);
void					set_arg(t_arg *arg);
int						f_s(t_arg *arg, t_buf *buf, va_list arg_list);
int						f_c(t_arg *arg, t_buf *buf, va_list arg_list);
int						f_percent(t_arg *arg, t_buf *buf, va_list arg_list);
int						f_p(t_arg *arg, t_buf *buf, va_list arg_list);
int						f_num_s(t_arg *arg, t_buf *buf, va_list arg_list);
int						f_num_u(t_arg *arg, t_buf *buf, va_list arg_list);
char					*ft_ulltoa(unsigned long long n);
int						ft_itoa_base_u(unsigned long long n, char *base_to
	, t_buf *buf, t_arg *arg);
int						ft_pad(t_arg *arg, t_buf *buf);
void					*easymalloc(size_t size);
int						ft_abs(int a);
int						ft_atoi(const char *str);
int						ft_atoi_base(char *nbr, char *base_from);
long					ft_atou_mv(char **str, char opt);
void					ft_bzero(void *s, size_t n);
long long int			ft_pow(long long int nb, long long int pow);
int						ft_check_base(char *str, char *base);
char					*ft_convert_base(char *nbr, char *base_from
	, char *base_to);
int						ft_find_next_prime(long unsigned nb);
void					ft_free(void **match);
void					easyfree(void **match);
int						ft_free_tab(void **tab, size_t len);
void					ft_free_gc(void);
int						ft_isalnum(int c);
int						ft_isalpha(int c);
int						ft_isascii(int c);
int						ft_isdigit(int c);
int						ft_isprint(int c);
int						ft_isspace(int c);
char					*ft_itoa(long long n);
char					*ft_itoa_base(long int nb, char *base_to);
int						ft_lstcycle(t_lst *begin);
void					ft_lstjoin(t_head *head_dest, t_head *head_src);
t_head					*ft_lstnew(void *content);
t_head					*ft_lstcpy(t_head *original_head);
t_lst					*ft_lstnew_elem(void *content);
t_head					*ft_lstnew_head(t_lst *first, t_lst *last);
void					ft_lstadd(t_head *head, t_lst *newl);
void					ft_lstadd_sorted(t_head *head, t_lst *newl
	, int (*cmp)(t_lst *, t_lst *));
void					ft_lstadd_back(t_head *head, t_lst *newl);
t_lst					*ft_lstadd_new(t_head *head, void *content);
t_lst					*ft_lstadd_back_new(t_head *head, void *content);
t_lst					*ft_lstpop(t_head *head, void *match);
void					ft_lsttransfer(t_head *head_from, t_head *heaad_to
	, t_lst *elem);
t_lst					*ft_lstfind(t_head *head, void *match);
void					ft_lstfree(t_head **head, int opt_elems, int opt_head);
void					ft_lstfree_elem(t_lst **elem, int opt);
void					ft_lstfree_head(t_head **head);
void					ft_lstinsert(t_head *head, t_lst *ref, t_lst *insert
	, int bef_or_aft);
void					ft_lstsort(t_head *head
	, int (*f_cmp)(t_lst *, t_lst *));
void					ft_lstswap_contents(t_lst *elem_1, t_lst *elem_2);
void					ft_lstswap_heads(t_head *head, t_lst *elem_1
	, t_lst *elem_2);
void					ft_lst_replace_elem(t_head *head, t_lst *original
	, t_lst *replacement);
int						ft_max(int a, int b);
int						ft_memchr_pos(const void *s, int c, size_t n);
void					*ft_memset(void *b, int c, size_t len);
void					ft_memcset(void *o_s, int c, int stop);
int						ft_memcmp(const void *s1, const void *s2, size_t n);
void					*ft_memcpy(void *dst, const void *src, size_t n);
void					*ft_memccpy(void *dst, const void *src, int c
	, size_t n);
void					*ft_memmove(void *dst, const void *src, size_t len);
void					*ft_memalloc(size_t size);
void					ft_memdel(void **ap);
int						ft_min(int a, int b);
void					ft_putchar(char c);
void					ft_putchar_fd(char c, int fd);
void					ft_putendl(char const *s);
void					ft_putendl_fd(char const *str, int fd);
void					ft_putnbr(int n);
void					ft_putnbr_fd(int n, int fd);
void					ft_putstr(char const *s);
int						ft_putstr_ret(char const *s);
void					ft_putstr_fd(char const *str, int fd);
int						ft_putstr_fd_ret(char const *str, int fd);
int						ft_strcmp(const char *s1, const char *s2);
int						ft_strncmp(const char *s1, const char *s2, size_t n);
int						ft_strcpy(char *dest, char *src);
char					*ft_strdup(const char *s1);
size_t					ft_strlen(const char *s);
size_t					ft_strlen_v4(const char *s);
char					*ft_strncpy(char *dst, const char *src, size_t len);
char					*ft_strrev(char *str);
char					*ft_strstr(const char *haystack, const char *needle);
char					*ft_strchr(const char *str, int c);
int						ft_strchr_pos(char *str, int c);
char					*ft_strrchr(const char *str, int c);
int						ft_strequ(char const *s1, char const *s2);
int						ft_strnequ(char const *s1, char const *s2, size_t n);
char					*ft_strsub(char const *s, unsigned int start
	, size_t len);
char					*ft_strjoin(char *s1, char *s2);
char					*ft_strjoin_free(char **s1, char **s2, int opt);
char					*ft_strtrim(char const *s);
char					*ft_strnew(size_t size);
void					ft_strdel(char **as);
void					ft_strclr(char *s);
char					**ft_strsplit(char const *s, char c);
void					ft_print_words_tables(char **tab);
void					ft_swap(void **a, void **b, int opt);
void					ft_swapstr(char **a, char **b);
void					ft_lstprint(t_head *head, char *name, unsigned opt);
int						get_gc_data(int opt);
t_head					*get_gc_list(int opt);
t_h_table				*ft_hash_new_table(void);
int						ft_hash_add_elem(t_h_table *table, char *key
	, void *content);
int						ft_hash_resize_array(t_h_table *table
	, unsigned new_size);
void					*ft_hash_get_content(t_h_table *table, char *key);
t_h_elem				*ft_hash_get_elem(t_h_table *table, char *key);
void					ft_hash_free_elem(t_h_table *table, t_h_elem *hash_elem
	, int opt);
t_h_elem				*ft_hash_pop_elem(t_h_table *table, char *key);
void					ft_hash_free_table(t_h_table *table, int opt);
int						ft_hash_iter(t_h_table *table, t_h_iter_func iter_func);
void					hash_print_elem(t_h_elem *elem);
void					hash_print_index(t_h_table *table, unsigned index
	, int opt);
void					hash_print_table(t_h_table *table, int opt);

#endif
