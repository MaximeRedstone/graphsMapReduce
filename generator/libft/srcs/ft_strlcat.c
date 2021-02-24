/* ************************************************************************** */
/*                                                                            */
/*                                                        :::      ::::::::   */
/*   ft_strlcat.c                                       :+:      :+:    :+:   */
/*                                                    +:+ +:+         +:+     */
/*   By: sikpenou <marvin@42.fr>                    +#+  +:+       +#+        */
/*                                                +#+#+#+#+#+   +#+           */
/*   Created: 2019/04/04 22:56:26 by sikpenou          #+#    #+#             */
/*   Updated: 2019/04/06 21:54:16 by sikpenou         ###   ########.fr       */
/*                                                                            */
/* ************************************************************************** */

#include "libft.h"

unsigned int	ft_strlcat(char *dst, const char *src, size_t size)
{
	char			*cpy_d;
	const char		*cpy_s;
	size_t			n;
	size_t			dlen;

	cpy_d = (char *)dst;
	cpy_s = src;
	n = size;
	while (n-- != 0 && *cpy_d != '\0')
		cpy_d++;
	dlen = cpy_d - dst;
	n = size - dlen;
	if (n == 0)
		return (dlen + ft_strlen((char *)cpy_s));
	while (*cpy_s != '\0')
	{
		if (n != 1)
		{
			*cpy_d++ = *cpy_s;
			n--;
		}
		cpy_s++;
	}
	*cpy_d = '\0';
	return (dlen + (cpy_s - src));
}
