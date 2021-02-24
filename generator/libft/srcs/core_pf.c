/* ************************************************************************** */
/*                                                                            */
/*                                                        :::      ::::::::   */
/*   core_pf.c                                          :+:      :+:    :+:   */
/*                                                    +:+ +:+         +:+     */
/*   By: sikpenou <marvin@42.fr>                    +#+  +:+       +#+        */
/*                                                +#+#+#+#+#+   +#+           */
/*   Created: 2019/09/30 20:26:52 by sikpenou          #+#    #+#             */
/*   Updated: 2020/01/03 14:43:57 by sikpenou         ###   ########.fr       */
/*                                                                            */
/* ************************************************************************** */

#include "libft.h"

int		init_structs(t_buf **buf, t_arg **arg)
{
	if (!(*buf = (t_buf *)easymalloc(sizeof(**buf))))
		return (0);
	(*buf)->tab[0] = &f_percent;
	(*buf)->tab[1] = &f_c;
	(*buf)->tab[2] = &f_s;
	(*buf)->tab[3] = &f_p;
	(*buf)->tab[4] = &f_num_s;
	(*buf)->tab[5] = &f_num_u;
	if (!(*arg = (t_arg *)easymalloc(sizeof(**arg))))
		return (0);
	return (1);
}

void	set_arg(t_arg *arg)
{
	ft_memset(arg, 0, sizeof(*arg));
	arg->prefix = "";
}

int		format_to_str(const char *str, t_buf *buf, t_arg *arg, va_list arg_lst)
{
	int				ret;
	unsigned int	str_pos;

	str_pos = 0;
	while (str[str_pos])
	{
		if (buf->pos == buf->lim
			&& !(buf->lim = ft_realloc((void *)&(buf->str), buf->lim, BLOCK)))
			return (-1);
		if (str[str_pos] == '%')
		{
			set_arg(arg);
			if ((ret = parse_arg(buf, arg, str + str_pos + 1, arg_lst)) == -1)
				return (-1);
			str_pos += ret + 1;
		}
		else
		{
			buf->str[buf->pos++] = str[str_pos++];
		}
	}
	return (1);
}

t_buf	*core_func(const char *str, va_list arg_lst)
{
	int		check;
	t_buf	*buf;
	t_arg	*arg;

	buf = NULL;
	arg = NULL;
	if (!init_structs(&buf, &arg))
		return (NULL);
	if ((check = format_to_str(str, buf, arg, arg_lst)) == -1)
		return (NULL);
	if (buf->str)
		buf->str[buf->pos] = 0;
	easyfree((void **)&arg);
	return (buf);
}
