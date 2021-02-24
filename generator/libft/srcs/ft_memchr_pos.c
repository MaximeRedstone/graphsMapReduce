/* ************************************************************************** */
/*                                                                            */
/*                                                        :::      ::::::::   */
/*   ft_memchr_pos.c                                    :+:      :+:    :+:   */
/*                                                    +:+ +:+         +:+     */
/*   By: sikpenou <marvin@42.fr>                    +#+  +:+       +#+        */
/*                                                +#+#+#+#+#+   +#+           */
/*   Created: 2019/04/10 13:01:55 by sikpenou          #+#    #+#             */
/*   Updated: 2019/04/10 13:04:19 by sikpenou         ###   ########.fr       */
/*                                                                            */
/* ************************************************************************** */

#include <string.h>

int		ft_memchr_pos(const void *s, int c, size_t n)
{
	unsigned char	*str;

	if (!(str = (unsigned char *)s))
		return (-1);
	while (n)
	{
		if (*str == (unsigned char)c)
			return (str - (unsigned char *)s);
		str++;
		n--;
	}
	return (c ? -1 : str - (unsigned char *)s);
}
