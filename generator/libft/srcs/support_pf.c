/* ************************************************************************** */
/*                                                                            */
/*                                                        :::      ::::::::   */
/*   support_pf.c                                       :+:      :+:    :+:   */
/*                                                    +:+ +:+         +:+     */
/*   By: sikpenou <marvin@42.fr>                    +#+  +:+       +#+        */
/*                                                +#+#+#+#+#+   +#+           */
/*   Created: 2019/09/30 20:27:53 by sikpenou          #+#    #+#             */
/*   Updated: 2019/09/30 20:29:35 by sikpenou         ###   ########.fr       */
/*                                                                            */
/* ************************************************************************** */

#include "libft.h"

int							add_to_buf(char *str, t_buf *buf)
{
	int		pos;

	pos = 0;
	while (str[pos])
	{
		if (!(buf->pos % BLOCK))
			if (!(buf->lim = ft_realloc((void **)&(buf->str), buf->lim,
				BLOCK)))
				return (0);
		buf->str[buf->pos++] = str[pos++];
	}
	easyfree((void **)&str);
	return (1);
}

unsigned long long int		ft_ato_ull(const char *str, int *pos)
{
	unsigned long long		nb;

	nb = 0;
	while (*str >= '0' && *str <= '9')
	{
		nb = nb * 10 + *str - 48;
		str++;
		(*pos)++;
	}
	(*pos)--;
	return (nb);
}
