/* ************************************************************************** */
/*                                                                            */
/*                                                        :::      ::::::::   */
/*   ft_memchrset.c                                     :+:      :+:    :+:   */
/*                                                    +:+ +:+         +:+     */
/*   By: sikpenou <marvin@42.fr>                    +#+  +:+       +#+        */
/*                                                +#+#+#+#+#+   +#+           */
/*   Created: 2019/04/05 14:53:09 by sikpenou          #+#    #+#             */
/*   Updated: 2019/04/11 14:13:43 by sikpenou         ###   ########.fr       */
/*                                                                            */
/* ************************************************************************** */

#include "libft.h"

void	*ft_memchrset(const void *o_s, const unsigned char *set, size_t n)
{
	const unsigned char		*s;
	int						len;

	if (!(s = (const unsigned char *)o_s) || !set || !n)
		return (NULL);
	len = ft_strlen((char *)set);
	while (n)
	{
		if (ft_memchr(set, *s, len))
			return ((void *)s);
		s++;
		n--;
	}
	return (NULL);
}
