/* ************************************************************************** */
/*                                                                            */
/*                                                        :::      ::::::::   */
/*   ft_printf.c                                        :+:      :+:    :+:   */
/*                                                    +:+ +:+         +:+     */
/*   By: sikpenou <marvin@42.fr>                    +#+  +:+       +#+        */
/*                                                +#+#+#+#+#+   +#+           */
/*   Created: 2019/07/02 00:23:34 by sikpenou          #+#    #+#             */
/*   Updated: 2019/09/30 19:43:00 by sikpenou         ###   ########.fr       */
/*                                                                            */
/* ************************************************************************** */

#include <unistd.h>
#include <stdarg.h>
#include "libft.h"

int		ft_printf(const char *str, ...)
{
	va_list		arg_lst;
	t_buf		*buf;
	int			ret;

	if (!str)
		return (0);
	va_start(arg_lst, str);
	buf = core_func(str, arg_lst);
	if (buf->str)
		write(1, buf->str, buf->pos);
	ret = buf->pos;
	easyfree((void **)&buf->str);
	easyfree((void **)&buf);
	return (ret);
}

int		ft_dprintf(int fd, const char *str, ...)
{
	va_list		arg_lst;
	t_buf		*buf;
	int			ret;

	if (!str)
		return (0);
	va_start(arg_lst, str);
	buf = core_func(str, arg_lst);
	if (buf->str && fd > 0)
		write(fd, buf->str, buf->pos);
	else
		write(2, "ft_dprintf error: invalid fd\n", 29);
	ret = buf->pos;
	easyfree((void **)&buf->str);
	easyfree((void **)&buf);
	return (ret);
}

int		ft_sprintf(char **dest, const char *str, ...)
{
	va_list		arg_lst;
	t_buf		*buf;
	int			ret;

	if (!str)
		return (0);
	va_start(arg_lst, str);
	buf = core_func(str, arg_lst);
	if (buf->str)
	{
		*dest = ft_strdup(buf->str);
	}
	ret = buf->pos;
	easyfree((void **)&buf->str);
	easyfree((void **)&buf);
	return (ret);
}
