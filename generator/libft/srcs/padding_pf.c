/* ************************************************************************** */
/*                                                                            */
/*                                                        :::      ::::::::   */
/*   padding_pf.c                                       :+:      :+:    :+:   */
/*                                                    +:+ +:+         +:+     */
/*   By: sikpenou <marvin@42.fr>                    +#+  +:+       +#+        */
/*                                                +#+#+#+#+#+   +#+           */
/*   Created: 2019/09/30 20:27:40 by sikpenou          #+#    #+#             */
/*   Updated: 2019/09/30 20:27:41 by sikpenou         ###   ########.fr       */
/*                                                                            */
/* ************************************************************************** */

#include "libft.h"

int		left_just(t_arg *arg, t_buf *buf)
{
	if (buf->pos < arg->start + arg->min)
		while (arg->start + arg->min >= buf->lim)
			if (!(buf->lim = ft_realloc((void *)&(buf->str), buf->lim
							, BLOCK)))
				return (-1);
	while (buf->pos < arg->start + arg->min)
	{
		buf->str[buf->pos] = arg->fill;
		buf->pos++;
	}
	return (0);
}

int		right_just(t_arg *arg, t_buf *buf)
{
	int				pos;
	int				off;

	arg->fill == '0' && buf->str[arg->start] == '-' ? arg->min-- : 0;
	arg->fill == '0' && buf->str[arg->start] == '-' ? arg->start++ : 0;
	arg->fill == '0' ? arg->start += ft_strlen(arg->prefix) : 0;
	arg->fill == '0' ? arg->min -= ft_strlen(arg->prefix) : 0;
	pos = arg->start + arg->min;
	off = pos - buf->pos;
	if (off < 1)
		return (0);
	while ((unsigned)pos >= buf->lim)
		if (!(buf->lim = ft_realloc((void *)&(buf->str), buf->lim, BLOCK)))
			return (-1);
	while ((unsigned)pos >= arg->start + off)
	{
		buf->str[pos] = buf->str[pos - off];
		pos--;
	}
	while (pos > -1 && (unsigned)pos >= arg->start)
		buf->str[pos--] = arg->fill;
	buf->pos = arg->start + arg->min;
	return (0);
}

int		ft_prefix(t_arg *arg, t_buf *buf)
{
	int		off;
	int		pos;

	off = arg->prefix[1] ? 2 : 1;
	while (buf->pos + off >= buf->lim)
		if (!(buf->lim = ft_realloc((void *)&(buf->str), buf->lim, BLOCK)))
			return (-1);
	buf->pos += off;
	pos = buf->pos - 1;
	while (pos >= (int)arg->start + off)
	{
		buf->str[pos] = buf->str[pos - off];
		pos--;
	}
	buf->str[arg->start] = arg->prefix[0];
	arg->prefix[1] ? (buf->str[arg->start + 1] = arg->prefix[1]) : 0;
	return (arg->flags & MINUS ? left_just(arg, buf) : right_just(arg, buf));
}

int		ft_pad(t_arg *arg, t_buf *buf)
{
	if (arg->flags & PLUS || arg->flags & SPACE)
		if ((arg->type != DTYPE) || (arg->flags & SPACE && arg->type & PLUS))
			return (-1);
	if (arg->flags & PLUS || arg->flags & SPACE)
		arg->prefix = arg->flags & PLUS ? "+" : " ";
	arg->prefix = buf->str[arg->start] == '-' ? "" : arg->prefix;
	if (arg->flags & HASH)
	{
		if (arg->type < DTYPE)
			return (-1);
		else if (arg->type == OTYPE && buf->str[arg->start] != '0')
			arg->prefix = "0";
		else if (arg->type == XTYPE)
			arg->prefix = "0x";
		else if (arg->type == XMAJTYPE)
			arg->prefix = "0X";
	}
	arg->fill = arg->flags & ZERO && !(arg->flags & MINUS || arg->prec)
		? '0' : ' ';
	if (arg->prefix[0] && buf->str[arg->start] != '-')
		return (ft_prefix(arg, buf));
	return (arg->flags & MINUS ? left_just(arg, buf) : right_just(arg, buf));
}
