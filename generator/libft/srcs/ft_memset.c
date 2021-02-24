/* ************************************************************************** */
/*                                                                            */
/*                                                        :::      ::::::::   */
/*   ft_memset.c                                        :+:      :+:    :+:   */
/*                                                    +:+ +:+         +:+     */
/*   By: sikpenou <marvin@42.fr>                    +#+  +:+       +#+        */
/*                                                +#+#+#+#+#+   +#+           */
/*   Created: 2019/04/05 14:50:03 by sikpenou          #+#    #+#             */
/*   Updated: 2019/10/24 15:21:11 by sikpenou         ###   ########.fr       */
/*                                                                            */
/* ************************************************************************** */

#include <stddef.h>

void	*ft_memset(void *b, int c, size_t len)
{
	long	*zone;
	long	*stop;
	long	pattern;

	if (!len)
		return (b);
	zone = (long *)b;
	if (len >= sizeof(long))
	{
		pattern = c;
		while (c && !(pattern >> 55))
			pattern = (pattern << 8) + c;
		stop = (long *)b + len / sizeof(long);
		while (zone < stop)
			*zone++ = pattern;
		len %= sizeof(long);
	}
	while (len)
		*((char *)zone + --len) = (unsigned char)c;
	return (b);
}
