/* ************************************************************************** */
/*                                                                            */
/*                                                        :::      ::::::::   */
/*   num_funcs_pf.c                                     :+:      :+:    :+:   */
/*                                                    +:+ +:+         +:+     */
/*   By: sikpenou <marvin@42.fr>                    +#+  +:+       +#+        */
/*                                                +#+#+#+#+#+   +#+           */
/*   Created: 2019/09/30 20:27:32 by sikpenou          #+#    #+#             */
/*   Updated: 2019/09/30 20:27:35 by sikpenou         ###   ########.fr       */
/*                                                                            */
/* ************************************************************************** */

#include "libft.h"

int		ft_itoa_base_s(long long int n, char *base_to, t_buf *buf, t_arg *arg)
{
	int					pos;
	int					blen;
	long long int		keep;

	pos = 0;
	keep = n;
	blen = ft_strlen(base_to);
	while ((keep || (unsigned)pos < arg->max) && ++pos)
		keep /= blen;
	pos = pos + (n < 1 ? 1 : 0);
	pos = pos - (!n && arg->max ? 1 : 0);
	while (buf->pos + pos >= buf->lim)
		if (!(buf->lim = ft_realloc((void *)&(buf->str), buf->lim, BLOCK)))
			return (-1);
	pos += buf->pos;
	buf->str[buf->pos] = n < 0 ? '-' : 0;
	keep = pos;
	while (pos > 0 && buf->pos - pos > arg->max)
	{
		if (buf->str[pos - 1] != '-')
			buf->str[pos - 1] = base_to[(n < 0 ? -(n % blen) : n % blen)];
		n /= blen;
		pos--;
	}
	return (keep);
}

char	*get_base(long type)
{
	if (type == UTYPE || type == DTYPE)
		return ("0123456789");
	else if (type == OTYPE)
		return ("01234567");
	else if (type == XTYPE)
		return ("0123456789abcdef");
	else if (type == XMAJTYPE)
		return ("0123456789ABCDEF");
	else
		return ("01");
}

int		f_num_s(t_arg *arg, t_buf *buf, va_list arg_lst)
{
	long long	n;
	char		*base;

	n = 0;
	if (!(arg->field))
		n = (int)va_arg(arg_lst, int);
	else if (arg->field == HFIELD)
		n = (short)va_arg(arg_lst, int);
	else if (arg->field == HHFIELD)
		n = (char)va_arg(arg_lst, int);
	else if (arg->field == LFIELD)
		n = (long int)va_arg(arg_lst, long int);
	else
		n = (long long int)va_arg(arg_lst, long long int);
	base = get_base(arg->type);
	if (!arg->prec || arg->max || n)
		buf->pos = ft_itoa_base_s(n, base, buf, arg);
	if (buf->pos == -1)
		return (-1);
	return (ft_pad(arg, buf));
}

int		ft_itoa_base_u(unsigned long long n, char *base_to, t_buf *buf
	, t_arg *arg)
{
	int							pos;
	int							blen;
	unsigned long long int		keep;

	pos = 0;
	keep = n;
	blen = ft_strlen(base_to);
	while ((keep || (unsigned)pos < arg->max) && ++pos)
		keep /= blen;
	pos = pos + (n < 1 ? 1 : 0);
	pos = pos - (!n && arg->max ? 1 : 0);
	while (buf->pos + pos >= buf->lim)
		if (!(buf->lim = ft_realloc((void *)&(buf->str), buf->lim, BLOCK)))
			return (-1);
	pos += buf->pos;
	buf->str[buf->pos] = n ? buf->str[buf->pos] : *base_to;
	keep = pos;
	while (pos > (int)arg->start && (!arg->prec || buf->pos - pos > arg->max))
	{
		if (buf->str[pos - 1] != '-')
			buf->str[pos - 1] = base_to[n % blen];
		n /= blen;
		pos--;
	}
	return (keep);
}

int		f_num_u(t_arg *arg, t_buf *buf, va_list arg_lst)
{
	long long int		n;
	char				*base;

	if (!arg->field)
		n = (unsigned int)va_arg(arg_lst, int);
	else if (arg->field == HFIELD)
		n = (unsigned short)va_arg(arg_lst, int);
	else if (arg->field == HHFIELD)
		n = (unsigned char)va_arg(arg_lst, int);
	else if (arg->field == LFIELD)
		n = (unsigned long int)va_arg(arg_lst, long int);
	else
		n = (unsigned long long int)va_arg(arg_lst, long long int);
	!n && arg->flags & HASH && (arg->type != OTYPE || !arg->prec)
		? (arg->flags ^= HASH) : 0;
	base = get_base(arg->type);
	if (!arg->prec || arg->max || n)
		buf->pos = ft_itoa_base_u(n, base, buf, arg);
	if (buf->pos == -1)
		return (-1);
	return (ft_pad(arg, buf));
}
