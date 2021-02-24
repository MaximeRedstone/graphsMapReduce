/* ************************************************************************** */
/*                                                                            */
/*                                                        :::      ::::::::   */
/*   text_funcs_pf.c                                    :+:      :+:    :+:   */
/*                                                    +:+ +:+         +:+     */
/*   By: sikpenou <marvin@42.fr>                    +#+  +:+       +#+        */
/*                                                +#+#+#+#+#+   +#+           */
/*   Created: 2019/09/30 20:28:19 by sikpenou          #+#    #+#             */
/*   Updated: 2019/09/30 22:21:41 by sikpenou         ###   ########.fr       */
/*                                                                            */
/* ************************************************************************** */

#include "libft.h"

int		f_s(t_arg *arg, t_buf *buf, va_list arg_lst)
{
	char		*str;
	unsigned	len;

	len = 0;
	if (!(str = (char *)va_arg(arg_lst, char *)))
		str = "(null)";
	while (str[len] && (arg->max ? len < arg->max : 1))
	{
		if (!(buf->pos % BLOCK))
			if (!(buf->lim = ft_realloc((void *)&(buf->str), buf->lim
						, BLOCK)))
				return (-1);
		buf->str[buf->pos] = str[len];
		buf->pos++;
		len++;
	}
	return (ft_pad(arg, buf));
}

int		f_c(t_arg *arg, t_buf *buf, va_list arg_lst)
{
	char	c;

	c = (char)va_arg(arg_lst, int);
	if (!(buf->pos % BLOCK))
		if (!(buf->lim = ft_realloc((void *)&(buf->str), buf->lim
					, BLOCK + 1)))
			return (-1);
	buf->str[buf->pos++] = c;
	return (ft_pad(arg, buf));
}

int		f_percent(t_arg *arg, t_buf *buf, va_list arg_lst)
{
	(void)arg_lst;
	arg->flags & PLUS ? (arg->flags ^= PLUS) : 0;
	arg->flags & SPACE ? (arg->flags ^= SPACE) : 0;
	if (!(buf->pos % BLOCK))
		if (!(buf->lim = ft_realloc((void *)&(buf->str), buf->lim, BLOCK)))
			return (-1);
	buf->str[buf->pos++] = '%';
	return (ft_pad(arg, buf));
}

int		f_p(t_arg *arg, t_buf *buf, va_list arg_lst)
{
	long long int	addr;
	char			*nil;

	if (arg->max || arg->field || arg->flags & ~MINUS)
		return (-1);
	if (!(addr = (unsigned long long int)va_arg(arg_lst, long int)))
	{
		nil = "0x0";
		if (buf->pos + 3 > buf->lim
			&& !(buf->lim = ft_realloc((void *)&(buf->str), buf->lim, BLOCK)))
			return (-1);
		while (*nil)
			buf->str[buf->pos++] = *(nil++);
	}
	else
	{
		arg->prefix = "0x";
		buf->pos = ft_itoa_base_u(addr, "0123456789abcdef", buf, arg);
	}
	return (ft_pad(arg, buf));
}
