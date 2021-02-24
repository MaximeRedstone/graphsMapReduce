/* ************************************************************************** */
/*                                                                            */
/*                                                        :::      ::::::::   */
/*   parsing_pf.c                                       :+:      :+:    :+:   */
/*                                                    +:+ +:+         +:+     */
/*   By: sikpenou <marvin@42.fr>                    +#+  +:+       +#+        */
/*                                                +#+#+#+#+#+   +#+           */
/*   Created: 2019/09/30 20:26:52 by sikpenou          #+#    #+#             */
/*   Updated: 2019/09/30 22:21:23 by sikpenou         ###   ########.fr       */
/*                                                                            */
/* ************************************************************************** */

#include "libft.h"

int			get_field(t_arg *arg, const char *str, int pos, int check)
{
	if ((check = ft_strchr_pos(FIELD, str[pos])) > -1)
	{
		if ((str[pos] == 'h' || str[pos] == 'l') && str[pos + 1] == str[pos])
		{
			check++;
			pos++;
		}
		arg->field = check + 1;
	}
	return (pos);
}

int			get_size(va_list arg_lst, const char *str, int *pos, long *prec)
{
	unsigned long long int		ret;

	if (str[*pos] == '*')
	{
		ret = (int)va_arg(arg_lst, int);
	}
	else
	{
		ret = ft_ato_ull(str, pos);
	}
	if (prec)
		*prec = 1;
	if (ret > 2000000)
		return (-1);
	else if ((int)ret < 0)
		ret = 0;
	return (ret);
}

int			parse_flags(t_arg *arg, const char *str, va_list arg_lst)
{
	int		pos;
	int		check;

	pos = -1;
	while (str[++pos] && ft_strchr_pos(ALL, str[pos]) > -1)
	{
		if ((check = ft_strchr_pos(FLAGS, str[pos])) > -1)
			arg->flags |= (1 << check);
		else if (ft_isdigit(str[pos]) || str[pos] == '*')
		{
			arg->min = get_size(arg_lst, str + pos, &pos, 0);
			if (arg->min == -1)
				return (-1);
		}
		else if ((str[pos] == '.' || str[pos] == '*') && ++pos)
		{
			arg->max = get_size(arg_lst, str + pos, &pos, &arg->prec);
			if (arg->max == -1)
				return (-1);
		}
		else if ((check = ft_strchr_pos(FIELD, str[pos])) > -1)
			pos = get_field(arg, str, pos, check);
	}
	return (pos);
}

int			parse_arg(t_buf *buf, t_arg *arg, const char *str
	, va_list arg_lst)
{
	int		pos;
	int		check;

	if ((pos = parse_flags(arg, str, arg_lst)) == -1)
		return (-1);
	check = str[pos] == 'i' ? (int)DTYPE : ft_strchr_pos(TYPE, str[pos]);
	if (check < 0)
		return (-1);
	arg->type = check;
	arg->start = buf->pos;
	if (arg->type > DTYPE)
		check = buf->tab[DTYPE + 1](arg, buf, arg_lst);
	else
		check = buf->tab[arg->type](arg, buf, arg_lst);
	if (check == -1)
		return (-1);
	return (pos + 1);
}
